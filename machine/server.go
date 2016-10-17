package machine

import (
	"fmt"
	"io"
	"os"
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

type backupWriter struct {
	conn redcon.DetachedConn
}

func (wr *backupWriter) Write(p []byte) (n int, err error) {
	wr.conn.WriteRaw(p)
	if err = wr.conn.Flush(); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (m *Machine) doBackup(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// BACKUP
	// BACKUP / HTTP/N
	var http bool
	switch len(cmd.Args) {
	default:
		return nil, finn.ErrWrongNumberOfArguments
	case 1:
	case 3:
		if !strings.HasPrefix(strings.ToLower(string(cmd.Args[2])), "http/") {
			return nil, errSyntaxError
		}
		http = true
	}
	f, err := os.Open(m.file)
	if err != nil {
		return nil, err
	}
	sz, err := f.Seek(0, 2)
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		return nil, err
	}
	go func(wr *backupWriter) {
		var err error
		defer func() {
			f.Close()
			conn.Close()
		}()
		if http {
			_, err = fmt.Fprintf(wr, ""+
				"HTTP/1.0 200 OK\r\n"+
				"Content-Length: %d\r\n"+
				"Content-Type: application/octet-stream\r\n"+
				"Content-Disposition: attachment; filename=\"backup.db\"\r\n"+
				"\r\n", sz)
		} else {
			_, err = fmt.Fprintf(wr, "$%d\r\n", sz)
		}
		if err != nil {
			return
		}
		if _, err = io.CopyN(wr, f, sz); err != nil {
			return
		}
		if !http {
			fmt.Fprintf(wr, "\r\n")
		}
	}(&backupWriter{conn.Detach()})
	return nil, nil
}
