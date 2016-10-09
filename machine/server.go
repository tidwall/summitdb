package machine

import (
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

func (m *Machine) doTime(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		nano := time.Duration(time.Now().UnixNano())
		secs := nano / time.Second
		micro := (nano - secs*time.Second) / time.Microsecond
		conn.WriteArray(2)
		conn.WriteBulkString(strconv.FormatInt(int64(secs), 10))
		conn.WriteBulkString(strconv.FormatInt(int64(micro), 10))
		return nil
	})
}

func (m *Machine) doDbsize(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		n, err := tx.Len()
		if err != nil {
			return err
		}
		if err := tx.AscendGreaterOrEqual("", sdbMetaPrefix, func(key, val string) bool {
			if !strings.HasPrefix(key, sdbMetaPrefix) {
				return false
			}
			n--
			return true
		}); err != nil {
			return err
		}
		conn.WriteInt(n)
		return nil
	})
}
