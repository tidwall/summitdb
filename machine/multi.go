package machine

import (
	"errors"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

type multiContext struct {
	writable bool
	errs     bool
	cmds     []redcon.Command
}

func (m *Machine) doMulti(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	if conn == nil {
		return nil, errors.New("missing connection")
	}
	ctx := conn.Context().(*connContext)
	ctx.multi = &multiContext{}
	conn.WriteString("OK")
	return nil, nil
}

func (m *Machine) doExec(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	if conn == nil {
		return nil, errors.New("missing connection")
	}
	ctx := conn.Context().(*connContext)
	defer func() {
		ctx.multi = nil
	}()
	if ctx.multi.errs {
		return nil, errors.New("EXECABORT Transaction discarded because of previous errors.")
	}
	if len(ctx.multi.cmds) == 0 {
		conn.WriteArray(0)
		return nil, nil
	}
	var args [][]byte
	if ctx.multi.writable {
		args = append(args, []byte("plwmulti"))
	} else {
		args = append(args, []byte("plrmulti"))
	}
	for _, cmd := range ctx.multi.cmds {
		args = append(args, cmd.Raw)
	}
	ctx.multi = nil
	ncmd := buildCommand(args)
	return m.doPlmulti(a, conn, ncmd, tx)
}

func (m *Machine) doDiscard(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	if conn == nil {
		return nil, errors.New("missing connection")
	}
	ctx := conn.Context().(*connContext)
	ctx.multi = nil
	conn.WriteString("OK")
	return nil, nil
}

func (m *Machine) doPlmulti(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	writable := qcmdlower(cmd.Args[0]) == "plwmulti"

	// read the commands
	var cmds []redcon.Command
	for i := 1; i < len(cmd.Args); i++ {
		cmd, err := parseCommand(cmd.Args[i])
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, cmd)
	}

	dowr := func(tx *buntdb.Tx) (interface{}, error) {
		var resps []interface{}
		for _, cmd := range cmds {
			pconn := &passiveConn{}
			_, err := m.doTransactableCommand(&passiveApplier{log: m.log}, pconn, cmd, tx)
			if err != nil {
				resps = append(resps, err)
			} else {
				resps = append(resps, pconn.resps...)
			}
		}
		return resps, nil
	}

	dord := func(v interface{}) error {
		conn.WriteArray(len(cmds))
		for _, resp := range v.([]interface{}) {
			switch v := resp.(type) {
			default:
				conn.WriteError("ERR invalid response")
			case string:
				conn.WriteString(v)
			case int64:
				conn.WriteInt64(v)
			case []byte:
				conn.WriteBulk(v)
			case error:
				conn.WriteError(v.Error())
			case []int:
				conn.WriteArray(v[0])
			}
		}
		return nil
	}

	if writable {
		return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
			return dowr(tx)
		}, func(v interface{}) error {
			return dord(v)
		})
	}

	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		v, err := dowr(tx)
		if err != nil {
			return err
		}
		return dord(v)
	})
}
