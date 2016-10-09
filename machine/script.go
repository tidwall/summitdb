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

// scriptMachine represents a global javascript VM and
// contains all of the runContexts.
type scriptMachine struct {
	mu      sync.Mutex
	vm      *otto.Otto
	runCtxs map[string]*runContext // run contexts
}

// newScriptMachine creates a new scriptMachine that is shared
// for all EVAL calls.
func newScriptMachine(m *Machine) (*scriptMachine, error) {
	sm := &scriptMachine{}
	sm.vm = otto.New()
	sm.runCtxs = make(map[string]*runContext)

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
				var SummitDB = function(sha, runid){
					this.sha = sha;
					this.runid = runid;
				}
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
		v, err := call.Otto.Get("sdb")
		if err != nil {
			panic(scriptErrPrefix + err.Error())
		}
		v, err = v.Object().Get("runid")
		if err != nil {
			panic(scriptErrPrefix + err.Error())
		}
		runid := v.String()
		ctx := sm.runCtxs[runid]
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
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		// get the sha1
		sha := func() string {
			src := sha1.Sum(cmd.Args[2])
			return hex.EncodeToString(src[:])
		}()
		javascript := `(function(){` + string(cmd.Args[2]) + `})();`
		var err error
		func() {
			m.sm.mu.Lock()
			defer m.sm.mu.Unlock()
			_, err = m.sm.vm.Compile("(new function)", javascript)
		}()
		if err != nil {
			return nil, fmt.Errorf("ERR Error compiling script %v", err)
		}
		_, _, err = tx.Set(scriptKeyPrefix+sha, string(cmd.Args[2]), nil)
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
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
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

	dowr := func(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
		var sha string
		var script string
		if shad {
			sha = string(cmd.Args[1])
			val, err := tx.Get(scriptKeyPrefix + sha)
			if err != nil {
				if err == buntdb.ErrNotFound {
					return nil, errors.New("NOSCRIPT No matching script. Please use EVAL.")
				}
				return nil, err
			}
			script = val
		} else {
			// evaluate the sha1
			sha = func() string {
				src := sha1.Sum(cmd.Args[1])
				return hex.EncodeToString(src[:])
			}()
			script = string(cmd.Args[1])
		}
		// create a run id
		run := func() string {
			nsrc := make([]byte, 20)
			if _, err := rand.Read(nsrc); err != nil {
				panic("random err: " + err.Error())
			}
			return hex.EncodeToString(nsrc)
		}()
		m.sm.mu.Lock()
		// each eval gets it own context
		vm := m.sm.vm.Copy()
		m.sm.runCtxs[run] = &runContext{
			tx:   tx,
			conn: &passiveConn{},
			a:    &passiveApplier{log: m.log},
		}
		m.sm.mu.Unlock()
		defer func() {
			m.sm.mu.Lock()
			delete(m.sm.runCtxs, run)
			m.sm.mu.Unlock()
		}()
		var v interface{}
		var err error
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
						panic(v)
					}
				}
			}()
			vm.Set("KEYS", keys)
			vm.Set("ARGV", argv)
			vm.Run(`var sdb = new SummitDB('` + sha + `','` + run + `')`)
			v, err = vm.Run(`(function(){` + script + `})();`)
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
