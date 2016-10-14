package machine

import (
	"strconv"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

func (m *Machine) doFence(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// FENCE token
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := sdbMetaPrefix + "fence:" + string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		var n uint64
		val, err := tx.Get(key)
		if err != nil {
			if err != buntdb.ErrNotFound {
				return nil, err
			}
		} else {
			n, err = strconv.ParseUint(val, 10, 64)
			if err != nil {
				return nil, err
			}
		}
		n++
		val = strconv.FormatUint(n, 10)
		_, _, err = tx.Set(key, val, nil)
		if err != nil {
			return nil, err
		}
		return val, nil
	}, func(v interface{}) error {
		conn.WriteBulkString(v.(string))
		return nil
	})
}
