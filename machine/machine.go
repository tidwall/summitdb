package machine

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

type Machine struct {
	log  finn.Logger
	sm   *scriptMachine
	addr string

	mu   sync.RWMutex
	db   *buntdb.DB
	file string
}

func New(log finn.Logger, addr string) (*Machine, error) {
	m := &Machine{log: log, addr: addr}
	err := m.reopenBlankDB(nil, func(keys []string) { m.onExpired(keys) })
	if err != nil {
		return nil, err
	}
	m.sm, err = newScriptMachine(m)
	if err != nil {
		m.Close()
		return nil, err
	}
	return m, nil
}

func (m *Machine) Close() error {
	return m.db.Close()
}

func (m *Machine) onExpired(keys []string) {
	// Connect to ourself using a standard redcon connection.
	// This is important in order to emulate a full round trip
	// through the Raft pipeline.
	// This operation only works on the leader. Followers will
	// simply receive a TRY response, which we will ignore.
	// This should not be a bottleneck because there cannot be
	// more than one connection at a time and never more that
	// one connection every one second.
	// Failures are ignored, but logged.
	err := func() error {
		m.log.Debugf("expire: %v", keys)
		conn, err := net.Dial("tcp", m.addr)
		if err != nil {
			return err
		}
		defer conn.Close()
		wr := redcon.NewWriter(conn)
		wr.WriteArray(len(keys) + 1)
		wr.WriteBulkString("del")
		for i := 0; i < len(keys); i++ {
			wr.WriteBulkString(keys[i])
		}
		err = wr.Flush()
		if err != nil {
			return err
		}
		rd := bufio.NewReader(conn)
		c, err := rd.ReadByte()
		if err != nil {
			return err
		}
		if c == '-' {
			line, err := rd.ReadString('\n')
			if err != nil {
				return err
			}
			m.log.Debugf("expired: failed: %v", strings.TrimSpace(line))
			return nil
		}
		if c == ':' {
			line, err := rd.ReadString('\n')
			if err != nil {
				return err
			}
			m.log.Debugf("expired: success: %v", strings.TrimSpace(line))
			return nil
		}
		m.log.Warningf("expired: success: %v", "invalid response")
		return nil
	}()
	if err != nil {
		m.log.Warningf("expired: error: %v", err)
		println(err.Error())
	}
}

type connContext struct {
	multi *multiContext
}

func (m *Machine) ConnAccept(conn redcon.Conn) bool {
	conn.SetContext(&connContext{})
	return true
}

func (m *Machine) ConnClosed(conn redcon.Conn, err error) {

}

func (m *Machine) reopenBlankDB(rd io.Reader, onExpired func(keys []string)) error {
	dir, err := ioutil.TempDir("", "summitdb")
	if err != nil {
		return err
	}
	file := path.Join(dir, "data.db")
	if rd != nil {
		f, err := os.Create(file)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := io.Copy(f, rd); err != nil {
			os.RemoveAll(file)
			return err
		}
		f.Close()
	}
	db, err := buntdb.Open(file)
	if err != nil {
		return err
	}
	var cfg buntdb.Config
	if err := db.ReadConfig(&cfg); err != nil {
		return err
	}
	cfg.OnExpired = func(keys []string) { onExpired(keys) }
	if err := db.SetConfig(cfg); err != nil {
		return err
	}
	if m.file != "" {
		os.RemoveAll(m.file)
	}
	if m.db != nil {
		m.db.Close()
	}
	m.file = file
	m.db = db
	return nil
}

func scriptNotAllowedCommand(cmd string) bool {
	switch strings.ToLower(cmd) {
	case "multi", "exec", "discard", "eval", "evalro", "evalsha", "evalsharo", "script":
		return true
	}
	return false
}

func respPipeline(conn redcon.Conn, pn int, err error) (interface{}, error) {
	if err != nil {
		if conn != nil {
			for i := 0; i < pn-1; i++ {
				conn.WriteError(err.Error())
			}
		}
	}
	return nil, err
}

// Command processes a command through the Raft pipeline.
func (m *Machine) Command(a finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if conn != nil {
		ctx, ok := conn.Context().(*connContext)
		if ok && ctx.multi != nil {
			// only EXEC, DISCARD, and Scriptable Commands allowed inside a multi
			switch qcmdlower(cmd.Args[0]) {
			default:
				v, err := m.doTransactableCommand(a, conn, cmd, nil)
				if err != nil {
					ctx.multi.errs = true
				}
				return v, err
			case "multi":
				return nil, errors.New("ERR MULTI calls can not be nested")
			case "exec":
				return m.doExec(a, conn, cmd, nil)
			case "discard":
				return m.doDiscard(a, conn, cmd, nil)
			}
		}
	}

	var pn int
	var err error
	// try to pipeline the command first.
	pn, cmd, err = pipelineCommand(conn, cmd)
	if err != nil {
		return nil, err
	}
	switch qcmdlower(cmd.Args[0]) {
	default:
		return m.doTransactableCommand(a, conn, cmd, nil)
	case "plget":
		// PLGET key [key ...]
		_, err := m.doMget(a, conn, cmd, nil)
		return respPipeline(conn, pn, err)
	case "plset":
		// PLSET key value [key value ...]
		_, err := m.doMset(a, conn, cmd, nil)
		return respPipeline(conn, pn, err)
	case "plwmulti", "plrmulti":
		// PLWMULTI cmd [cmd ...]
		// PLRMULTI cmd [cmd ...]
		return m.doPlmulti(a, conn, cmd, nil)
	case "massinsert":
		// MASSINSERT count
		return m.doMassInsert(a, conn, cmd, nil)
	case "multi":
		// MULTI
		return m.doMulti(a, conn, cmd, nil)
	case "exec":
		return nil, errors.New("ERR EXEC without MULTI")
	case "discard":
		return nil, errors.New("ERR DISCARD without MULTI")
	}
}

// doTransactableCommand will execute a command that can run from inside a MULTI command.
// Only commands that can safely be wrapped in a buntdb transaction can occure here.
func (m *Machine) doTransactableCommand(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	switch qcmdlower(cmd.Args[0]) {
	default:
		return m.doScriptableCommand(a, conn, cmd, tx)
	case "eval", "evalro", "evalsha", "evalsharo":
		// EVAL script numkeys [key ...] [arg ...]
		// EVALRO script numkeys [key ...] [arg ...]
		// EVALSHA sha1 numkeys [key ...] [arg ...]
		// EVALSHARO sha1 numkeys [key ...] [arg ...]
		return m.doEval(a, conn, cmd, nil)
	case "script":
		// SCRIPT LOAD script
		// SCRIPT FLUSH
		return m.doScript(a, conn, cmd, nil)
	}
}

// doScriptableCommand will execute a command that can run from inside an EVAL call.
// Only commands that can safely be wrapped in a buntdb transaction can occure here.
func (m *Machine) doScriptableCommand(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	switch qcmdlower(cmd.Args[0]) {
	default:
		return nil, finn.ErrUnknownCommand
	case "append":
		// APPEND key value
		return m.doAppend(a, conn, cmd, tx)
	case "decr", "incr", "decrby", "incrby":
		// DECR key
		// DECRBY key decrement
		// INCR key
		// INCRBY key increment
		return m.doIncr(a, conn, cmd, tx)
	case "incrbyfloat":
		// INCRBYFLOAT key increment
		return m.doIncrbyfloat(a, conn, cmd, tx)
	case "getset":
		// GETSET key value
		return m.doGetset(a, conn, cmd, tx)
	case "getrange":
		// GETRANGE key start end
		return m.doGetrange(a, conn, cmd, tx)
	case "setrange":
		// SETRANGE key offset value
		return m.doSetrange(a, conn, cmd, tx)
	case "bitcount":
		// BITCOUNT key [start end]
		return m.doBitcount(a, conn, cmd, tx)
	case "bitop":
		// BITOP operation destkey key [key ...]
		return m.doBitop(a, conn, cmd, tx)
	case "getbit":
		// GETBIT key offset
		return m.doGetbit(a, conn, cmd, tx)
	case "setbit":
		// SETBIT key offset value
		return m.doSetbit(a, conn, cmd, tx)
	case "bitpos":
		// BITPOS key bit [start] [end]
		return m.doBitpos(a, conn, cmd, tx)
	case "get":
		// GET key
		return m.doGet(a, conn, cmd, tx)
	case "set", "setex", "setnx", "psetex":
		// SET key value [EX seconds] [PX milliseconds] [NX|XX]
		// SETEX key seconds value
		// SETNX key value
		// PSETEX key milliseconds value
		return m.doSet(a, conn, cmd, tx)
	case "msetnx":
		// MSETNX key value [key value...]
		return m.doMsetnx(a, conn, cmd, tx)
	case "del":
		// DEL key [key ...]
		return m.doDel(a, conn, cmd, tx)
	case "pdel":
		// PDEL pattern
		return m.doPdel(a, conn, cmd, tx)
	case "mget":
		// MGET key [key ...]
		return m.doMget(a, conn, cmd, tx)
	case "mset":
		// MSET key value [key val ...]
		return m.doMset(a, conn, cmd, tx)
	case "strlen":
		// STRLEN key
		return m.doStrlen(a, conn, cmd, tx)
	case "keys", "iter":
		// KEYS pattern [PIVOT value] [LIMIT limit] [DESC|ASC] [WITHVALUES]
		// ITER index [PIVOT value] [LIMIT limit] [DESC|ASC] [RANGE min max] [MATCH pattern]
		return m.doIter(a, conn, cmd, tx)
	case "rect":
		// RECT index bounds [MATCH pattern] [SKIP skip] [LIMIT limit]
		return m.doRect(a, conn, cmd, tx)
	case "setindex":
		// SETINDEX name pattern SPATIAL [JSON path]
		// SETINDEX name pattern TEXT [CS] [COLLATE collate] [ASC|DESC]
		// SETINDEX name pattern JSON path [CS] [COLLATE collate] [ASC|DESC]
		// SETINDEX name pattern INT|FLOAT|UINT [ASC|DESC]
		// SETINDEX name pattern EVAL script
		return m.doSetIndex(a, conn, cmd, tx)
	case "delindex":
		// DELINDEX name
		return m.doDelIndex(a, conn, cmd, tx)
	case "indexes":
		// INDEXES pattern
		return m.doIndexes(a, conn, cmd, tx)
	case "flushdb", "flushall":
		// FLUSHDB
		// FLUSHALL
		return m.doFlushdb(a, conn, cmd, tx)
	case "type":
		// TYPE key
		return m.doType(a, conn, cmd, tx)
	case "dump":
		// DUMP key
		return m.doDump(a, conn, cmd, tx)
	case "restore":
		// RESTORE key ttl serialized-value [REPLACE]
		return m.doRestore(a, conn, cmd, tx)
	case "exists":
		// EXISTS key
		return m.doExists(a, conn, cmd, tx)
	case "ttl", "pttl":
		// TTL key
		// PTTL key
		return m.doTTL(a, conn, cmd, tx)
	case "expire", "expireat", "pexpire", "pexpireat":
		// EXPIRE key seconds
		// EXPIREAT key timestamp
		// PEXPIRE key milliseconds
		// PEXPIREAT key milliseconds-timestamp
		return m.doExpire(a, conn, cmd, tx)
	case "persist":
		// PERSIST key
		return m.doPersist(a, conn, cmd, tx)
	case "rename", "renamenx":
		// RENAME key newkey
		// RENAMENX key newkey
		return m.doRename(a, conn, cmd, tx)
	case "time":
		// TIME
		return m.doTime(a, conn, cmd, tx)
	case "dbsize":
		// DBSIZE
		return m.doDbsize(a, conn, cmd, tx)
	case "fence":
		// FENCE token
		return m.doFence(a, conn, cmd, tx)
	}
}
