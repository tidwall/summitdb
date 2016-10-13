package machine

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/robertkrimen/otto"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

const scriptErrPrefix = "eval err:"
const scriptKeyPrefix = sdbMetaPrefix + "script:"

var errNoScript = errors.New("NOSCRIPT No matching script. Please use EVAL.")

// scriptVM is is used to contain a compiled script and the parent otto VM.
type scriptVM struct {
	mu     sync.Mutex
	script *otto.Script
	vm     *otto.Otto
}

// scriptMachine represents a global javascript VM and
// contains all of the runContexts.
type scriptMachine struct {
	mu      sync.Mutex
	vm      *otto.Otto             // root vm
	cache   map[string]*scriptVM   // cache scripts
	runCtxs map[string]*runContext // run contexts
	log     finn.Logger            // main logger
}

// newScriptMachine creates a new scriptMachine that is shared
// for all EVAL calls.
func newScriptMachine(m *Machine) (*scriptMachine, error) {
	sm := &scriptMachine{}
	sm.vm = otto.New()
	sm.runCtxs = make(map[string]*runContext)
	sm.cache = make(map[string]*scriptVM)
	sm.log = m.log
	_, err := sm.vm.Run(`
			var SummitDB = (function(){
				var ErrorReply = function(err){
					this.err = err;
				}
				var StatusReply = function(ok){
					this.ok = ok;
				}
				ErrorReply.prototype.toString = function(){
					return this.err.toString();
				}
				StatusReply.prototype.toString = function(){
					return this.ok.toString();
				}
				var SummitDB = function(){}
				SummitDB.prototype.errorReply = function(err){
					return new ErrorReply(err);
				}
				SummitDB.prototype.statusReply = function(ok){
					return new StatusReply(ok);
				}
				SummitDB.prototype.toString = function(){
					return "[object SummitDB]";
				}
				return SummitDB;
			}())
			var sdb = new SummitDB();
			var redis = sdb;
		`)
	if err != nil {
		return nil, err
	}
	v, err := sm.vm.Get("SummitDB")
	if err != nil {
		return nil, err
	}
	v, err = v.Object().Get("prototype")
	if err != nil {
		return nil, err
	}
	proto := v.Object()
	sdbCall := func(name string, call otto.FunctionCall) otto.Value {
		v, err := call.Otto.Get("runid")
		if err != nil {
			panic(scriptErrPrefix + err.Error())
		}
		runid := v.String()
		sm.mu.Lock()
		ctx := sm.runCtxs[runid]
		sm.mu.Unlock()
		cmd := cmdFromArgs(call.Otto, call.ArgumentList)
		_, err = m.doScriptableCommand(ctx.a, ctx.conn, cmd, ctx.tx)
		if err != nil {
			if err == finn.ErrUnknownCommand {
				err = errors.New("ERR unknown command '" + string(cmd.Args[0]) + "'")
			} else if err == finn.ErrWrongNumberOfArguments {
				err = errors.New("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "'")
			}
			if name == "call" {
				panic(scriptErrPrefix + err.Error())
			}
			val, err := call.Otto.Call(`sdb.errorReply`, nil, err.Error())
			if err != nil {
				panic(scriptErrPrefix + err.Error())
			}
			return val
		}
		return jsValue(ctx.conn, call.Otto)
	}
	if err := proto.Set("call", func(call otto.FunctionCall) otto.Value {
		return sdbCall("call", call)
	}); err != nil {
		return nil, err
	}
	if err := proto.Set("pcall", func(call otto.FunctionCall) otto.Value {
		return sdbCall("pcall", call)
	}); err != nil {
		return nil, err
	}

	// redirect the console.log
	log := m.log
	console, err := sm.vm.Get("console")
	if err != nil {
		return nil, err
	}
	err = console.Object().Set("log", func(call otto.FunctionCall) otto.Value {
		args := call.ArgumentList
		if len(args) > 0 {
			var out string
			for i, arg := range args {
				if i > 0 {
					out += " "
				}
				val, _ := arg.Export()
				if !arg.IsPrimitive() {
					data, err := json.Marshal(val)
					if err == nil {
						val = string(data)
					}
				}
				out += fmt.Sprint(val)
			}
			log.Verbosef("eval: %s", out)
		}
		return otto.Value{}
	})
	if err != nil {
		return nil, err
	}
	return sm, nil
}

func (sm *scriptMachine) flushScripts() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.cache = make(map[string]*scriptVM)
}

func (sm *scriptMachine) compileScript(javascript string) (sha string, script *scriptVM, err error) {
	src := sha1.Sum([]byte(javascript))
	sha = hex.EncodeToString(src[:])
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var ok bool
	script = sm.cache[sha]
	if ok {
		return sha, script, nil
	}
	script = &scriptVM{vm: sm.vm.Copy()}
	script.script, err = script.vm.Compile("", "(function(){"+javascript+"})()")
	if err != nil {
		return "", script, err
	}
	sm.cache[sha] = script
	return sha, script, nil
}

func (sm *scriptMachine) getScript(sha string) *scriptVM {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.cache[sha]
}

func (sm *scriptMachine) addRunContext(runid string, tx *buntdb.Tx) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.runCtxs[runid] = &runContext{
		tx:   tx,
		conn: &passiveConn{},
		a:    &passiveApplier{log: sm.log},
	}
}
func (sm *scriptMachine) removeRunContext(runid string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.runCtxs, runid)
}

// runContext is used for unique values for each EVAL call.
type runContext struct {
	a    *passiveApplier
	conn *passiveConn
	tx   *buntdb.Tx
}

// cmdFromArgs creates a redcon.Command from javascript values
func cmdFromArgs(vm *otto.Otto, args []otto.Value) redcon.Command {
	if len(args) == 0 {
		v, err := otto.ToValue("")
		if err != nil {
			panic(err)
		}
		args = append(args, v)
	}
	// construct the command
	var poss []int
	var buf []byte
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		var s string
		if !arg.IsPrimitive() {
			narg, err := vm.Call(`JSON.stringify`, nil, arg)
			if err != nil {
				narg = arg
			}
			s = narg.String()
		} else if arg.IsBoolean() {
			t, _ := arg.ToBoolean()
			if t {
				s = "1"
			} else {
				s = "0"
			}
		} else {
			s = arg.String()
		}
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
		buf = append(buf, '\r', '\n')
		poss = append(poss, len(buf), len(buf)+len(s))
		buf = append(buf, s...)
		buf = append(buf, '\r', '\n')
	}
	var cmd redcon.Command
	cmd.Raw = buf
	for i := 0; i < len(poss); i += 2 {
		cmd.Args = append(cmd.Args, cmd.Raw[poss[i]:poss[i+1]])
	}
	return cmd
}

// jsValue reads through the scriptConn resps and generates a javascript value
// that can be returned to EVAL call for further processing.
func jsValue(conn *passiveConn, vm *otto.Otto) otto.Value {
	if len(conn.resps) == 0 {
		return otto.Value{}
	}
	resp := conn.resps[0]
	conn.resps = conn.resps[1:]
	var val otto.Value
	switch v := resp.(type) {
	case string:
		val, _ = vm.Call("sdb.statusReply", nil, v)
	case int64:
		val, _ = vm.ToValue(v)
	case []byte:
		val, _ = vm.ToValue(string(v))
	case error:
		val, _ = vm.Call("sdb.errorReply", nil, v)
	case []int:
		n := v[0]
		o, _ := vm.Object(`([])`)
		for i := 0; i < n; i++ {
			val := jsValue(conn, vm)
			o.Call("push", val)
		}
		val = o.Value()
	}
	return val
}

func (m *Machine) doScript(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	v, err := func() (interface{}, error) {
		switch qcmdlower(cmd.Args[1]) {
		default:
			return nil, finn.ErrWrongNumberOfArguments
		case "load":
			// SCRIPT LOAD script
			return m.doScriptLoad(a, conn, cmd, tx)
		case "flush":
			// SCRIPT FLUSH
			return m.doScriptFlush(a, conn, cmd, tx)
		}
	}()
	if err != nil {
		if err == finn.ErrWrongNumberOfArguments {
			err = errors.New("ERR Unknown SCRIPT subcommand or wrong # of args.")
		}
		return nil, err
	}
	return v, nil
}

func (m *Machine) doScriptLoad(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SCRIPT LOAD script
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	javascript := string(cmd.Args[2])
	// compile script and store in cache. We don't need the compiled script yet,
	// but it's ready when we need it.
	sha, _, err := m.sm.compileScript(javascript)
	if err != nil {
		return nil, fmt.Errorf("ERR Error compiling script %v", err)
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		// store the script in the database.
		_, _, err = tx.Set(scriptKeyPrefix+sha, javascript, nil)
		if err != nil {
			return nil, err
		}
		return sha, nil
	}, func(v interface{}) error {
		conn.WriteBulkString(v.(string))
		return nil
	})
}

func (m *Machine) doScriptFlush(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SCRIPT FLUSH
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	// remove all compiled script
	m.sm.flushScripts()
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		// delete from the database too
		var keys []string
		if err := tx.AscendGreaterOrEqual("", scriptKeyPrefix, func(key, val string) bool {
			if !strings.HasPrefix(key, scriptKeyPrefix) {
				return false
			}
			keys = append(keys, key)
			return true
		}); err != nil {
			return nil, err
		}
		for _, key := range keys {
			if _, err := tx.Delete(key); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, func(v interface{}) error {
		conn.WriteString("OK")
		return nil
	})
}

func (m *Machine) doEval(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// EVAL script numkeys [key ...] [arg ...]
	// EVALRO script numkeys [key ...] [arg ...]
	// EVALSHA sha1 numkeys [key ...] [arg ...]
	// EVALSHARO sha1 numkeys [key ...] [arg ...]
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	// should we use writable transactions?
	var writable bool
	var shad bool
	switch qcmdlower(cmd.Args[0]) {
	default:
		return nil, finn.ErrUnknownCommand
	case "eval":
		writable = true
	case "evalro":
	case "evalsha":
		writable = true
		shad = true
	case "evalsharo":
		shad = true
	}

	// get keys and argv
	keys, argv, err := func() (keys, argv []string, err error) {
		var n uint64
		n, err = strconv.ParseUint(string(cmd.Args[2]), 10, 64)
		if err != nil {
			err = errors.New("ERR value is not an integer or out of range")
			return
		}
		if int(n) > len(cmd.Args)-3 {
			err = errors.New("ERR Number of keys can't be greater than number of args")
			return
		}
		for i := 0; i < int(n); i++ {
			keys = append(keys, string(cmd.Args[3+i]))
		}
		for i := 3 + int(n); i < len(cmd.Args); i++ {
			argv = append(argv, string(cmd.Args[i]))
		}
		return
	}()
	if err != nil {
		return nil, err
	}
	// create a run id
	nsrc := make([]byte, 20)
	if _, err := rand.Read(nsrc); err != nil {
		panic("random err: " + err.Error())
	}
	runid := hex.EncodeToString(nsrc)

	// get the script ahead of time
	var sha string
	var javascript string
	var script *scriptVM
	if shad {
		sha = string(cmd.Args[1])
		script = m.sm.getScript(sha)
		if script == nil {
			// we do not have a script, but that's ok cause it's possible
			// the script exists in the database.
		}
	} else {
		javascript = string(cmd.Args[1])
		var err error
		sha, script, err = m.sm.compileScript(javascript)
		if err != nil {
			return nil, err
		}
		// we have a script and a sha.
	}
	dowr := func(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
		var err error
		if script == nil {
			// load the javascript from the database
			javascript, err = tx.Get(scriptKeyPrefix + sha)
			if err != nil {
				if err == buntdb.ErrNotFound {
					return nil, errNoScript
				}
				return nil, err
			}
			// we have a sha, javascript, but no compiled script. compile now
			_, script, err = m.sm.compileScript(javascript)
			if err != nil {
				// an error here may be really bad becuase it means that somehow
				// bad javascript was added to the database. just return the
				// error until we have a better strategy.
				return nil, err
			}
			// yay. we now have a sha, javascript and a compiled script.
		}

		// create a run context.
		m.sm.addRunContext(runid, tx)
		defer m.sm.removeRunContext(runid)

		var v interface{}
		func() {
			defer func() {
				if v := recover(); v != nil {
					if s, ok := v.(string); ok && strings.HasPrefix(s, scriptErrPrefix) {
						err = errors.New(s[len(scriptErrPrefix):])
						if strings.HasPrefix(err.Error(), "ERR unknown command '") {
							commandName := strings.ToLower(strings.Split(strings.Split(err.Error(), "'")[1], "'")[0])
							if scriptNotAllowedCommand(commandName) {
								err = errors.New("ERR command not allowed from script '" + commandName + "'")
							}
						}
					} else {
						m.log.Warningf("script panic: %v", v)
						panic(v)
					}
				}
			}()
			// lock the script. it can only run one at a time.
			script.mu.Lock()
			defer script.mu.Unlock()
			script.vm.Set("runid", runid)
			script.vm.Set("sha", sha)
			script.vm.Set("KEYS", keys)
			script.vm.Set("ARGV", argv)
			v, err = script.vm.Run(script.script)
		}()
		return v, err
	}
	dord := func(v interface{}) error {
		val, err := otto.ToValue(v)
		if err != nil {
			return err
		}
		return writeValToConn(m.sm.vm, val, conn)
	}
	if writable {
		return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
			return dowr(a, conn, cmd, tx)
		}, func(v interface{}) error {
			return dord(v)
		})
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		v, err := dowr(a, conn, cmd, tx)
		if err != nil {
			return err
		}
		return dord(v)
	})
}

// writeValToConn write a javascript value to the client connection
func writeValToConn(vm *otto.Otto, val otto.Value, conn redcon.Conn) error {
	if val.IsNull() || val.IsUndefined() {
		conn.WriteNull()
		return nil
	}
	if val.IsString() {
		s, _ := val.ToString()
		conn.WriteBulkString(s)
		return nil
	}
	if val.IsNumber() {
		n, err := val.ToFloat()
		if err != nil {
			return err
		}
		conn.WriteInt(int(n))
		return nil
	}
	if val.IsObject() {
		// When the return value has the signature {type:"string",value:"?"}
		// then a simple string is written to the client with the value
		obj := val.Object()
		res, _ := vm.Call(`Array.isArray`, nil, val)
		isarr, _ := res.ToBoolean()
		if isarr {
			res, _ := obj.Get("length")
			length, _ := res.ToInteger()
			conn.WriteArray(int(length))
			for i := 0; i < int(length); i++ {
				v, _ := obj.Get(strconv.FormatInt(int64(i), 10))
				if err := writeValToConn(vm, v, conn); err != nil {
					return err
				}
			}
			return nil
		}

		if val, err := obj.Get("err"); err == nil && val.IsDefined() {
			s, _ := val.ToString()
			conn.WriteError(s)
			return nil
		}

		if val, err := obj.Get("ok"); err == nil && val.IsDefined() {
			s, _ := val.ToString()
			conn.WriteString(s)
			return nil
		}
	}

	vv, _ := val.Export()
	data, _ := json.Marshal(vv)
	conn.WriteBulk(data)
	return nil
}
