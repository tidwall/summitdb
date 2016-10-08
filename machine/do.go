package machine

import (
	"errors"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
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
