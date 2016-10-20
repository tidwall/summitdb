package machine

import (
	"fmt"
	"strconv"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
	"github.com/tidwall/sjson"
)

func (m *Machine) doJget(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// JGET key path
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil {
			if err == buntdb.ErrNotFound {
				conn.WriteNull()
				return nil
			}
			return err
		}
		res := gjson.Get(val, string(cmd.Args[2]))
		if !res.Exists() {
			conn.WriteNull()
			return nil
		}
		conn.WriteBulkString(res.String())
		return nil
	})
}
func (m *Machine) doJset(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// JSET key path value [RAW]
	var raw, str bool
	switch len(cmd.Args) {
	default:
		return nil, finn.ErrWrongNumberOfArguments
	case 4:
	case 5:
		switch qcmdlower(cmd.Args[4]) {
		default:
			return nil, errSyntaxError
		case "raw":
			raw = true
		case "str":
			str = true
		}
	}
	key := string(cmd.Args[1])
	path := string(cmd.Args[2])
	val := string(cmd.Args[3])
	if !str && !raw {
		switch val {
		default:
			if len(val) > 0 {
				if (val[0] >= '0' && val[0] <= '9') || val[0] == '-' {
					if _, err := strconv.ParseFloat(val, 64); err == nil {
						raw = true
					}
				}
			}
		case "true", "false", "null":
			raw = true
		}
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		json, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		if raw {
			// set as raw block
			json, err = sjson.SetRaw(json, path, val)
		} else {
			// set as a string
			json, err = sjson.Set(json, path, val)
		}
		if err != nil {
			return nil, fmt.Errorf("ERR %v", err)
		}
		_, _, err = tx.Set(key, json, nil)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}, func(v interface{}) error {
		conn.WriteString("OK")
		return nil
	})
}
func (m *Machine) doJdel(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// JDEL key path
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := string(cmd.Args[1])
	path := string(cmd.Args[2])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		json, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return 0, nil
			}
			return nil, err
		}
		res, err := sjson.Delete(json, path)
		if err != nil {
			return nil, fmt.Errorf("ERR %v", err)
		}
		if res != json {
			_, _, err = tx.Set(key, res, nil)
			if err != nil {
				return nil, err
			}
			return 1, nil
		}
		return 0, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
	return nil, nil
}
