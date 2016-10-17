package machine

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

func (m *Machine) doGet(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// GET key
	if len(cmd.Args) != 2 {
		if len(cmd.Args) == 3 {
			if string(cmd.Args[1]) == "/backup" && strings.HasPrefix(strings.ToLower(string(cmd.Args[2])), "http/") {
				return m.doBackup(a, conn, cmd, tx)
			}
		}
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
		conn.WriteBulkString(val)
		return nil
	})
}

func (m *Machine) doStrlen(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// STRLEN key
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		conn.WriteInt(len(val))
		return nil
	})
}
func (m *Machine) doSet(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SET key value [EX seconds] [PX milliseconds] [NX|XX]
	// SETEX key seconds value
	// SETNX key value
	// PSETEX key milliseconds value
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	commandName := qcmdlower(cmd.Args[0])
	if len(cmd.Args) == 3 && commandName == "set" {
		// fasttrack
		return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
			_, _, err := tx.Set(string(cmd.Args[1]), string(cmd.Args[2]), nil)
			return nil, err
		}, func(v interface{}) error {
			conn.WriteString("OK")
			return nil
		})
	}
	var key, val string
	var px, nx, xx bool
	var pxi int
	switch commandName {
	default:
		return nil, finn.ErrUnknownCommand
	case "set":
		key = string(cmd.Args[1])
		val = string(cmd.Args[2])
		for i := 3; i < len(cmd.Args); i++ {
			switch qcmdlower(cmd.Args[i]) {
			case "ex", "px":
				if px {
					return nil, errSyntaxError
				}
				i++
				if i >= len(cmd.Args) {
					return nil, errSyntaxError
				}
				n, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
				if err != nil {
					return nil, errNotAnInt
				}
				if n <= 0 {
					return nil, errors.New("ERR invalid expire time in set")
				}
				px = true
				if qcmdlower(cmd.Args[i-1]) == "ex" {
					pxi = int(n) * 1000
				} else {
					pxi = int(n)
				}
			case "nx":
				if nx || xx {
					return nil, errSyntaxError
				}
				nx = true
			case "xx":
				if nx || xx {
					return nil, errSyntaxError
				}
				xx = true
			}
		}
	case "setex", "psetex":
		key = string(cmd.Args[1])
		val = string(cmd.Args[3])
		n, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		if err != nil {
			return nil, errNotAnInt
		}
		if n <= 0 {
			return nil, errors.New("ERR invalid expire time in setex")
		}
		px = true
		if qcmdlower(cmd.Args[0]) == "setex" {
			pxi = int(n) * 1000
		} else {
			pxi = int(n)
		}
	case "setnx":
		key = string(cmd.Args[1])
		val = string(cmd.Args[2])
		nx = true
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if nx {
			_, err := tx.Get(key)
			if err == nil {
				return nil, nil
			}
			if err != buntdb.ErrNotFound {
				return nil, err
			}
		} else if xx {
			_, err := tx.Get(key)
			if err != nil {
				if err == buntdb.ErrNotFound {
					return nil, nil
				}
				return nil, err
			}
		}
		var opts *buntdb.SetOptions
		if px {
			opts = &buntdb.SetOptions{}
			opts.Expires = true
			opts.TTL = time.Millisecond * time.Duration(pxi)
		}
		_, _, err := tx.Set(key, val, opts)
		return "OK", err
	}, func(v interface{}) error {
		if v == nil {
			conn.WriteNull()
		} else {
			conn.WriteString(v.(string))
		}
		return nil
	})
}

func (m *Machine) doMset(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 == 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	pipeline := qcmdlower(cmd.Args[0]) == "plset"
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		for i := 1; i < len(cmd.Args); i += 2 {
			_, _, err := tx.Set(string(cmd.Args[i]), string(cmd.Args[i+1]), nil)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, func(v interface{}) error {
		if !pipeline {
			conn.WriteString("OK")
		} else {
			for i := 1; i < len(cmd.Args); i += 2 {
				conn.WriteString("OK")
			}
		}
		return nil
	})
}

func (m *Machine) doMsetnx(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// MSETNX key value [key value ...]
	if len(cmd.Args) < 3 || (len(cmd.Args)-1)%2 == 1 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		for i := 1; i < len(cmd.Args); i += 2 {
			key := string(cmd.Args[i])
			_, err := tx.Get(key)
			if err == nil {
				return 0, nil
			}
			if err != buntdb.ErrNotFound {
				return nil, err
			}
			_, _, err = tx.Set(key, string(cmd.Args[i+1]), nil)
			if err != nil {
				return nil, err
			}
		}
		return 1, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doMget(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// MGET key [key ...]
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	pipeline := qcmdlower(cmd.Args[0]) == "plget"
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		vals := make([]*string, 0, len(cmd.Args)-1)
		for i := 1; i < len(cmd.Args); i++ {
			val, err := tx.Get(string(cmd.Args[i]))
			if err != nil {
				if err == buntdb.ErrNotFound {
					vals = append(vals, nil)
					continue
				}
				return err
			}
			vals = append(vals, &val)
		}
		if !pipeline {
			conn.WriteArray(len(vals))
		}
		for _, val := range vals {
			if val == nil {
				conn.WriteNull()
			} else {
				conn.WriteBulkString(*val)
			}
		}
		return nil
	})
}
func (m *Machine) doAppend(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// APPEND key value
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		val += string(cmd.Args[2])
		_, _, err = tx.Set(key, val, nil)
		if err != nil {
			return nil, err
		}
		return len(val), nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}
func (m *Machine) doIncr(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// DECR key
	// DECRBY key decrement
	// INCR key
	// INCRBY key increment
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	var amt int64
	switch qcmdlower(cmd.Args[0]) {
	default:
		return nil, finn.ErrUnknownCommand
	case "decr":
		amt = -1
	case "incr":
		amt = +1
	case "decrby", "incrby":
		if len(cmd.Args) != 3 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		n, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
		if err != nil {
			return nil, errNotAnInt
		}
		if qcmdlower(cmd.Args[0]) == "decrby" {
			amt = -n
		} else {
			amt = +n
		}
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		var n int64
		if val != "" {
			n, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, errNotAnInt
			}
		}
		n += amt
		val = strconv.FormatInt(n, 10)
		_, _, err = tx.Set(key, val, nil)
		if err != nil {
			return nil, err
		}
		return n, nil
	}, func(v interface{}) error {
		conn.WriteInt64(v.(int64))
		return nil
	})
}
func (m *Machine) doIncrbyfloat(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// INCRBYFLOAT key increment
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	amt, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return nil, errors.New("ERR value is not a valid float")
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		var n float64
		if val != "" {
			n, err = strconv.ParseFloat(val, 64)
			if err != nil {
				return nil, errors.New("ERR value is not a valid float")
			}
		}
		n += amt
		if math.IsNaN(n) || math.IsInf(n, +1) || math.IsInf(n, -1) {
			return nil, errors.New("ERR increment would produce NaN or Infinity")
		}
		val = strconv.FormatFloat(n, 'f', -1, 64)
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
func (m *Machine) doGetset(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// GETSET key value
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		exists := true
		val, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				exists = false
			} else {
				return nil, err
			}
		}
		_, _, err = tx.Set(key, string(cmd.Args[2]), nil)
		if err != nil {
			return nil, err
		}
		if exists {
			return val, nil
		}
		return nil, nil
	}, func(v interface{}) error {
		if v == nil {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(v.(string))
		}
		return nil
	})
}

func parseStartEnd(cmd redcon.Command, startIndex, endIndex int) (start, end int, err error) {
	if startIndex >= len(cmd.Args) || endIndex >= len(cmd.Args) {
		return 0, 0, errNotAnInt
	}
	n, err := strconv.ParseInt(string(cmd.Args[startIndex]), 10, 64)
	if err != nil {
		return 0, 0, errNotAnInt
	}
	start = int(n)
	n, err = strconv.ParseInt(string(cmd.Args[endIndex]), 10, 64)
	if err != nil {
		return 0, 0, errNotAnInt
	}
	end = int(n)
	return
}
func reevalStartEnd(start, end int, val string) (newStart, newEnd int) {
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}
	if start > len(val)-1 {
		return 0, 0
	}
	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > end {
		return 0, 0
	}
	return start, end + 1
}

func (m *Machine) doGetrange(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) != 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	start, end, err := parseStartEnd(cmd, 2, 3)
	if err != nil {
		return nil, err
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		start, end = reevalStartEnd(start, end, val)
		val = val[start:end]
		conn.WriteBulkString(val)
		return nil
	})
}

func (m *Machine) doSetrange(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SETRANGE key offset value
	if len(cmd.Args) != 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	n, err := strconv.ParseUint(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR offset is out of range")
	}
	offset := int(n)
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		bval := []byte(val)
		if len(bval) < offset+len(cmd.Args[3]) {
			// add empty space
			size := offset + len(cmd.Args[3])
			bval = append(bval, make([]byte, size-len(bval))...)
		}
		copy(bval[offset:], cmd.Args[3])

		val = string(bval)
		_, _, err = tx.Set(key, val, nil)
		if err != nil {
			return nil, err
		}
		return len(val), nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doBitcount(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	var rng bool
	var start, end int
	if len(cmd.Args) == 4 {
		var err error
		start, end, err = parseStartEnd(cmd, 2, 3)
		if err != nil {
			return nil, err
		}
		rng = true
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		if rng {
			start, end = reevalStartEnd(start, end, val)
			val = val[start:end]
		}
		var n int
		for i := 0; i < len(val); i++ {
			c := val[i]
			for j := uint(0); j < 8; j++ {
				if c>>j&1 == 1 {
					n++
				}
			}
		}
		conn.WriteInt(n)
		return nil
	})
}
func (m *Machine) doBitop(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// BITOP operation destkey key [key ...]
	if len(cmd.Args) < 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	const (
		opAnd = iota
		opOr
		opXor
		opNot
	)
	var op int
	switch qcmdlower(cmd.Args[1]) {
	default:
		return nil, errSyntaxError
	case "and":
		op = opAnd
	case "or":
		op = opOr
	case "xor":
		op = opXor
	case "not":
		op = opNot
		if len(cmd.Args) > 4 {
			return nil, errors.New("ERR BITOP NOT must be called with a single source key.")
		}
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if op == opNot {
			val, err := tx.Get(string(cmd.Args[3]))
			if err != nil && err != buntdb.ErrNotFound {
				return nil, err
			}
			nval := make([]byte, len(val))
			for i := 0; i < len(val); i++ {
				nval[i] = ^val[i]
			}
			_, _, err = tx.Set(string(cmd.Args[2]), string(nval), nil)
			if err != nil {
				return nil, err
			}
			return len(nval), nil
		}

		var maxlen int
		vals := make([]string, 0, len(cmd.Args)-3)
		for i := 3; i < len(cmd.Args); i++ {
			val, err := tx.Get(string(cmd.Args[i]))
			if err != nil && err != buntdb.ErrNotFound {
				return nil, err
			}
			vals = append(vals, val)
			if len(val) > maxlen {
				maxlen = len(val)
			}
		}
		for i := 0; i < len(vals); i++ {
			if len(vals[i]) < maxlen {
				vals[i] = string(append([]byte(vals[i]), make([]byte, maxlen-len(vals[i]))...))
			}
		}

		nval := []byte(vals[0])
		for i := 1; i < len(vals); i++ {
			val := vals[i]
			for j := 0; j < len(nval); j++ {
				switch op {
				case opAnd:
					nval[j] = nval[j] & val[j]
				case opOr:
					nval[j] = nval[j] | val[j]
				case opXor:
					nval[j] = nval[j] ^ val[j]
				}
			}
		}
		_, _, err := tx.Set(string(cmd.Args[2]), string(nval), nil)
		if err != nil {
			return nil, err
		}
		return len(nval), nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}
func (m *Machine) doGetbit(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// GETBIT key offset
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	offset, err := strconv.ParseUint(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR bit offset is not an integer or out of range")
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		var bit int64
		i := int(offset / 8)
		if i < len(val) {
			pos := uint(7 - (offset - (uint64(i) * 8)))
			bit = int64((val[i] >> pos) & 1)
		}
		conn.WriteInt64(bit)
		return nil
	})
}
func (m *Machine) doSetbit(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SETBIT key offset value
	if len(cmd.Args) != 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	offset, err := strconv.ParseUint(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, errors.New("ERR bit offset is not an integer or out of range")
	}
	bit, err := strconv.ParseUint(string(cmd.Args[3]), 10, 64)
	if err != nil || bit > 1 {
		return nil, errors.New("ERR bit is not an integer or out of range")
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		bval := []byte(val)
		i := int(offset / 8)
		if i >= len(bval) {
			bval = append(bval, make([]byte, i-len(bval)+1)...)
		}
		pos := uint(7 - (offset - (uint64(i) * 8)))
		obit := int((bval[i] >> pos) & 1)
		if int(obit) != int(bit) {
			bval[i] ^= 1 << pos
		}
		_, _, err = tx.Set(string(cmd.Args[1]), string(bval), nil)
		if err != nil {
			return nil, err
		}
		return obit, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}
func (m *Machine) doBitpos(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// BITPOS key bit [start] [end]
	if len(cmd.Args) < 3 || len(cmd.Args) > 5 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	bit, err := strconv.ParseUint(string(cmd.Args[2]), 10, 64)
	if err != nil || bit > 1 {
		return nil, errors.New("ERR bit is not an integer or out of range")
	}
	var start int
	var end int
	end = -1
	if len(cmd.Args) > 3 {
		var err error
		if len(cmd.Args) > 4 {
			start, end, err = parseStartEnd(cmd, 3, 4)
		} else {
			start, _, err = parseStartEnd(cmd, 3, 3)
		}
		if err != nil {
			return nil, err
		}
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		if len(val) == 0 {
			conn.WriteInt(-1)
			return nil
		}
		start, end = reevalStartEnd(start, end, val)
		val = val[start:end]
		var pos int
		for i := 0; i < len(val); i++ {
			c := val[i]
			for j := 0; j < 8; j++ {
				if uint64(c>>uint(8-j-1)&1) == bit {
					conn.WriteInt(start*8 + pos)
					return nil
				}
				pos++
			}
		}
		conn.WriteInt(-1)
		return nil
	})
}
