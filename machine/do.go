package machine

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

var (
	errSyntaxError   = errors.New("ERR syntax error")
	errKeyNotAllowed = errors.New("ERR key not allowed")
	errNotAnInt      = errors.New("ERR value is not an integer or out of range")
)

// validateApplier is used only to validate the command, aways succeeds.
type validateApplier struct{}

func (a *validateApplier) Apply(
	conn redcon.Conn, cmd redcon.Command,
	mutate func() (interface{}, error),
	respond func(interface{}) (interface{}, error),
) (interface{}, error) {
	return nil, nil
}

func (a *validateApplier) Log() finn.Logger {
	return nil
}

func (m *Machine) writeDoApply(
	a finn.Applier,
	conn redcon.Conn,
	cmd redcon.Command,
	tx *buntdb.Tx,
	wrdo func(tx *buntdb.Tx) (interface{}, error),
	rddo func(v interface{}) error,
) (interface{}, error) {
	if conn != nil {
		ctx, ok := conn.Context().(*connContext)
		if ok && ctx.multi != nil {
			ctx.multi.cmds = append(ctx.multi.cmds, cmd)
			ctx.multi.writable = wrdo != nil
			conn.WriteString("QUEUED")
			return nil, nil
		}
	}
	return a.Apply(conn, cmd, func() (v interface{}, err error) {
		if tx != nil {
			v, err = wrdo(tx)
		} else {
			err = m.db.Update(func(tx *buntdb.Tx) error {
				var err error
				v, err = wrdo(tx)
				return err
			})
		}
		return v, err
	}, func(v interface{}) (interface{}, error) {
		return nil, rddo(v)
	})
}

func (m *Machine) readDoApply(
	a finn.Applier,
	conn redcon.Conn,
	cmd redcon.Command,
	tx *buntdb.Tx,
	rddo func(tx *buntdb.Tx) error,
) (interface{}, error) {
	if conn != nil {
		ctx, ok := conn.Context().(*connContext)
		if ok && ctx.multi != nil {
			ctx.multi.cmds = append(ctx.multi.cmds, cmd)
			conn.WriteString("QUEUED")
			return nil, nil
		}
	}
	return a.Apply(conn, cmd, nil, func(v interface{}) (interface{}, error) {
		if tx != nil {
			return nil, rddo(tx)
		}
		return nil, m.db.View(func(tx *buntdb.Tx) error {
			return rddo(tx)
		})
	})
}

func (m *Machine) doType(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	if isMercMetaKeyBytes(cmd.Args[1]) {
		return nil, errKeyNotAllowed
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		_, err := tx.Get(string(cmd.Args[1]))
		if err != nil {
			if err == buntdb.ErrNotFound {
				conn.WriteString("none")
				return nil
			}
			return err
		}
		conn.WriteString("string")
		return nil
	})
}

func (m *Machine) doDump(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	return m.doGet(a, conn, cmd, tx)
}

func (m *Machine) doRestore(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// RESTORE key ttl serialized-value [REPLACE]
	if len(cmd.Args) < 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	n, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, errNotAnInt
	}
	if n < 0 {
		return nil, errors.New("ERR Invalid TTL value, must be >= 0")
	}
	ttl := time.Millisecond * time.Duration(n)
	var replace bool
	if len(cmd.Args) == 5 {
		if qcmdlower(cmd.Args[4]) != "replace" {
			return nil, errSyntaxError
		}
		replace = true
	}
	if isMercMetaKeyBytes(cmd.Args[1]) {
		return nil, errKeyNotAllowed
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if !replace {
			_, err := tx.Get(key)
			if err == nil {
				return nil, errors.New("BUSYKEY Target key name already exists.")
			}
			if err != buntdb.ErrNotFound {
				return nil, err
			}
		}
		var opts *buntdb.SetOptions
		if ttl > 0 {
			opts = &buntdb.SetOptions{}
			opts.Expires = true
			opts.TTL = ttl
		}
		_, _, err := tx.Set(key, string(cmd.Args[3]), opts)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}, func(v interface{}) error {
		conn.WriteString("OK")
		return nil
	})
}

func (m *Machine) doExists(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	for i := 1; i < len(cmd.Args); i++ {
		if isMercMetaKeyBytes(cmd.Args[i]) {
			return nil, errKeyNotAllowed
		}
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		var n int
		for i := 1; i < len(cmd.Args); i++ {
			_, err := tx.Get(string(cmd.Args[i]))
			if err != nil {
				if err == buntdb.ErrNotFound {
					continue
				}
				return err
			}
			n++
		}
		conn.WriteInt(n)
		return nil
	})
}

func (m *Machine) doRename(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// RENAME key newkey
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	nx := qcmdlower(cmd.Args[0]) == "renamenx"
	if isMercMetaKeyBytes(cmd.Args[1]) || isMercMetaKeyBytes(cmd.Args[2]) {
		return nil, errKeyNotAllowed
	}
	key := string(cmd.Args[1])
	newkey := string(cmd.Args[2])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if nx {
			_, err := tx.Get(newkey)
			if err == nil {
				return 0, nil
			}
			if err != buntdb.ErrNotFound {
				return nil, err
			}
		}
		val, err := tx.Delete(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return nil, errors.New("ERR no such key")
			}
			return nil, err
		}
		_, _, err = tx.Set(newkey, val, nil)
		if err != nil {
			return nil, err
		}
		if nx {
			return 1, nil
		}
		return nil, nil
	}, func(v interface{}) error {
		if nx {
			conn.WriteInt(v.(int))
		} else {
			conn.WriteString("OK")
		}
		return nil
	})
}

func (m *Machine) doPersist(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	if isMercMetaKeyBytes(cmd.Args[1]) {
		return nil, errKeyNotAllowed
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return 0, nil
			}
			return nil, err
		}
		_, _, err = tx.Set(key, val, nil)
		if err != nil {
			return nil, err
		}
		return 1, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}
func (m *Machine) doTTL(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// TTL key
	// PTTL key
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	var resolution time.Duration
	switch qcmdlower(cmd.Args[0]) {
	default:
		return nil, finn.ErrUnknownCommand
	case "ttl":
		resolution = time.Second
	case "pttl":
		resolution = time.Millisecond
	}
	if isMercMetaKeyBytes(cmd.Args[1]) {
		return nil, errKeyNotAllowed
	}
	key := string(cmd.Args[1])
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		ttl, err := tx.TTL(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				conn.WriteInt(-2)
				return nil
			}
			return err
		}
		if ttl < 0 {
			conn.WriteInt(-1)
			return nil
		}
		conn.WriteInt(int(ttl / resolution))
		return nil
	})
}

func (m *Machine) doExpire(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// EXPIRE key seconds
	// EXPIREAT key timestamp
	// PEXPIRE key milliseconds
	// PEXPIREAT key milliseconds-timestamp
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	var ttl time.Duration
	n, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, errNotAnInt
	}
	switch qcmdlower(cmd.Args[0]) {
	default:
		return nil, finn.ErrUnknownCommand
	case "expire":
		ttl = time.Second * time.Duration(n)
	case "pexpire":
		ttl = time.Millisecond * time.Duration(n)
	case "expireat":
		ttl = time.Unix(0, int64(time.Second*time.Duration(n))).Sub(time.Now())
	case "pexpireat":
		ttl = time.Unix(0, int64(time.Millisecond*time.Duration(n))).Sub(time.Now())
	}
	if isMercMetaKeyBytes(cmd.Args[1]) {
		return nil, errKeyNotAllowed
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return 0, nil
			}
			return nil, err
		}
		if ttl <= 0 {
			ttl = 0
		}
		_, _, err = tx.Set(key, val, &buntdb.SetOptions{Expires: true, TTL: ttl})
		if err != nil {
			return nil, err
		}
		return 1, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doDel(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// DEL key [key ...]
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	for i := 1; i < len(cmd.Args); i++ {
		if isMercMetaKeyBytes(cmd.Args[i]) {
			return nil, errKeyNotAllowed
		}
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		var n int
		for i := 1; i < len(cmd.Args); i++ {
			_, err := tx.Delete(string(cmd.Args[i]))
			if err != nil {
				if err == buntdb.ErrNotFound {
					continue
				}
				return nil, err
			}
			n++
		}
		return n, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

// flushAllButMeta removes all data from the database except meta keys.
func flushAllButMeta(tx *buntdb.Tx) ([]string, int, error) {
	// backup the meta keys
	var metas []string
	if err := tx.AscendGreaterOrEqual("", mercMetaPrefix, func(key, val string) bool {
		if !isMercMetaKey(key) {
			return false
		}
		metas = append(metas, key, val)
		return true
	}); err != nil {
		return nil, 0, err
	}
	n, err := tx.Len()
	if err != nil {
		return nil, 0, err
	}
	if err := tx.DeleteAll(); err != nil {
		return nil, 0, err
	}
	// add the meta keys back
	for i := 0; i < len(metas); i += 2 {
		_, _, err := tx.Set(metas[i], metas[i+1], nil)
		if err != nil {
			return nil, 0, err
		}
	}
	return metas, n - (len(metas) / 2), nil
}

func (m *Machine) doPdel(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// PDEL pattern
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	pattern := string(string(cmd.Args[1]))
	min, max := match.Allowable(pattern)
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if pattern == "*" {
			_, n, err := flushAllButMeta(tx)
			return n, err
		}
		var n int
		var keys []string
		if strings.HasPrefix(pattern, "*") {
			if err := tx.Ascend("", func(key, _ string) bool {
				if isMercMetaKey(key) {
					return true
				}
				if match.Match(key, pattern) {
					keys = append(keys, key)
				}
				return true
			}); err != nil {
				return nil, err
			}
		} else {
			if err := tx.AscendRange("", min, max, func(key, _ string) bool {
				if isMercMetaKey(key) {
					return true
				}
				if match.Match(key, pattern) {
					keys = append(keys, key)
				}
				return true
			}); err != nil {
				return nil, err
			}
		}
		for _, key := range keys {
			if isMercMetaKey(key) {
				continue
			}
			_, err := tx.Delete(key)
			if err != nil {
				if err == buntdb.ErrNotFound {
					continue
				}
				return nil, err
			}
			n++
		}
		return n, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

// doFlushDB deletes all items in the database
func (m *Machine) doFlushdb(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		// delete everything but the meta data.
		// the indexes also remain but are empty
		metas, _, err := flushAllButMeta(tx)
		if err != nil {
			return nil, err
		}
		// now delete the indexes
		for i := 0; i < len(metas); i += 2 {
			key := metas[i]
			if strings.HasPrefix(key, indexKeyPrefix) {
				if err := tx.DropIndex(key[len(indexKeyPrefix):]); err != nil {
					return nil, err
				}
				if _, err := tx.Delete(key); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}, func(v interface{}) error {
		conn.WriteString("OK")
		return nil
	})
}

// doMassInsert is for debugging only. it inserts a lot of data
func (m *Machine) doMassInsert(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// MASSINSERT count
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	num, err := strconv.ParseUint(string(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	n := int(num)
	if n > 100000000 {
		return nil, errSyntaxError
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		var kvs []string
		for i := 0; i < n; i++ {
			num := "0000000000" + strconv.FormatInt(int64(i), 10)
			num = num[len(num)-10:]
			kvs = append(kvs, "__key__:"+num, "__val__:"+num)
		}
		for i := 0; i < len(kvs); i += 2 {
			if _, _, err := tx.Set(kvs[i], kvs[i+1], nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, func(interface{}) error {
		conn.WriteInt(n)
		return nil
	})
}
