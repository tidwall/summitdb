package machine

import (
	"bufio"
	"bytes"
	"errors"
	"strconv"
	"strings"

	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

// This is a fixed value. It should never ever ever be changed"
const sdbMetaPrefix = "!141259cdc892c099cfa01ecabd8375362b6aa09d!"

func isMercMetaKeyBytes(key []byte) bool {
	if len(key) >= len(sdbMetaPrefix) {
		for i := 0; i < len(sdbMetaPrefix); i++ {
			if key[i] != sdbMetaPrefix[i] {
				return false
			}
		}
		return true
	}
	return false
}

func isMercMetaKey(key string) bool {
	if len(key) >= len(sdbMetaPrefix) {
		for i := 0; i < len(sdbMetaPrefix); i++ {
			if key[i] != sdbMetaPrefix[i] {
				return false
			}
		}
		return true
	}
	return false
}

// pipelineCommand creates a single command from a pipeline.
func pipelineCommand(conn redcon.Conn, cmd redcon.Command) (int, redcon.Command, error) {
	if conn == nil {
		return 0, cmd, nil
	}
	pcmds := conn.PeekPipeline()
	if len(pcmds) == 0 {
		return 0, cmd, nil
	}
	args := make([][]byte, 0, 64)
	switch qcmdlower(cmd.Args[0]) {
	default:
		return 0, cmd, nil
	case "plget", "plset":
		return 0, redcon.Command{}, finn.ErrUnknownCommand
	case "get":
		if len(cmd.Args) != 2 {
			return 0, cmd, nil
		}
		// convert to an PLGET command which similar to an MGET
		for _, pcmd := range pcmds {
			if qcmdlower(pcmd.Args[0]) != "get" || len(pcmd.Args) != 2 {
				return 0, cmd, nil
			}
		}
		args = append(args, []byte("plget"))
		for _, pcmd := range append([]redcon.Command{cmd}, pcmds...) {
			args = append(args, pcmd.Args[1])
		}
	case "set":
		if len(cmd.Args) != 3 {
			return 0, cmd, nil
		}
		// convert to a PLSET command which is similar to an MSET
		for _, pcmd := range pcmds {
			if qcmdlower(pcmd.Args[0]) != "set" || len(pcmd.Args) != 3 {
				return 0, cmd, nil
			}
		}
		args = append(args, []byte("plset"))
		for _, pcmd := range append([]redcon.Command{cmd}, pcmds...) {
			args = append(args, pcmd.Args[1], pcmd.Args[2])
		}
	}

	// remove the peeked items off the pipeline
	conn.ReadPipeline()

	ncmd := buildCommand(args)
	return len(pcmds) + 1, ncmd, nil
}

func buildCommand(args [][]byte) redcon.Command {
	// build a pipeline command
	buf := make([]byte, 0, 128)
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')

	poss := make([]int, 0, len(args)*2)
	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(arg)), 10)...)
		buf = append(buf, '\r', '\n')
		poss = append(poss, len(buf), len(buf)+len(arg))
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}

	// reformat a new command
	var ncmd redcon.Command
	ncmd.Raw = buf
	ncmd.Args = make([][]byte, len(poss)/2)
	for i, j := 0, 0; i < len(poss); i, j = i+2, j+1 {
		ncmd.Args[j] = ncmd.Raw[poss[i]:poss[i+1]]
	}
	return ncmd
}

func parseCommand(raw []byte) (redcon.Command, error) {
	var cmd redcon.Command
	cmd.Raw = raw
	pos := 0
	rd := bufio.NewReader(bytes.NewBuffer(raw))
	c, err := rd.ReadByte()
	if err != nil {
		return cmd, err
	}
	pos++
	if c != '*' {
		return cmd, errors.New("invalid command")
	}
	line, err := rd.ReadString('\n')
	if err != nil {
		return cmd, err
	}
	pos += len(line)
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return cmd, errors.New("invalid command")
	}
	n, err := strconv.ParseUint(line[:len(line)-2], 10, 64)
	if err != nil {
		return cmd, err
	}
	if n == 0 {
		return cmd, errors.New("invalid command")
	}
	for i := uint64(0); i < n; i++ {
		c, err := rd.ReadByte()
		if err != nil {
			return cmd, err
		}
		pos++
		if c != '$' {
			return cmd, errors.New("invalid command")
		}
		line, err := rd.ReadString('\n')
		if err != nil {
			return cmd, err
		}
		pos += len(line)
		if len(line) < 2 || line[len(line)-2] != '\r' {
			return cmd, errors.New("invalid command")
		}
		n, err := strconv.ParseUint(line[:len(line)-2], 10, 64)
		if err != nil {
			return cmd, err
		}
		if _, err := rd.Discard(int(n) + 2); err != nil {
			return cmd, err
		}
		s := pos
		pos += int(n) + 2
		if raw[pos-2] != '\r' || raw[pos-1] != '\n' {
			return cmd, errors.New("invalid command")
		}
		cmd.Args = append(cmd.Args, raw[s:pos-2])
	}
	return cmd, nil
}

// qcmdlower for common optimized command lowercase conversions.
func qcmdlower(n []byte) string {
	switch len(n) {
	case 3:
		if (n[0] == 's' || n[0] == 'S') &&
			(n[1] == 'e' || n[1] == 'E') &&
			(n[2] == 't' || n[2] == 'T') {
			return "set"
		}
		if (n[0] == 'g' || n[0] == 'G') &&
			(n[1] == 'e' || n[1] == 'E') &&
			(n[2] == 't' || n[2] == 'T') {
			return "get"
		}
	case 4:
		if (n[0] == 'm' || n[0] == 'M') &&
			(n[1] == 's' || n[1] == 'S') &&
			(n[2] == 'e' || n[2] == 'E') &&
			(n[3] == 't' || n[3] == 'T') {
			return "mset"
		}
		if (n[0] == 'm' || n[0] == 'M') &&
			(n[1] == 'g' || n[1] == 'G') &&
			(n[2] == 'e' || n[2] == 'E') &&
			(n[3] == 't' || n[3] == 'T') {
			return "mget"
		}
		if (n[0] == 'e' || n[0] == 'E') &&
			(n[1] == 'v' || n[1] == 'V') &&
			(n[2] == 'a' || n[2] == 'A') &&
			(n[3] == 'l' || n[3] == 'L') {
			return "eval"
		}
	case 5:
		if (n[0] == 'p' || n[0] == 'P') &&
			(n[1] == 'l' || n[1] == 'L') &&
			(n[2] == 's' || n[2] == 'S') &&
			(n[3] == 'e' || n[3] == 'E') &&
			(n[4] == 't' || n[4] == 'T') {
			return "plset"
		}
		if (n[0] == 'p' || n[0] == 'P') &&
			(n[1] == 'l' || n[1] == 'L') &&
			(n[2] == 'g' || n[2] == 'G') &&
			(n[3] == 'e' || n[3] == 'E') &&
			(n[4] == 't' || n[4] == 'T') {
			return "plget"
		}
	case 6:
		if (n[0] == 'e' || n[0] == 'E') &&
			(n[1] == 'v' || n[1] == 'V') &&
			(n[2] == 'a' || n[2] == 'A') &&
			(n[3] == 'l' || n[3] == 'L') &&
			(n[4] == 'r' || n[4] == 'R') &&
			(n[5] == 'o' || n[5] == 'O') {
			return "evalro"
		}
	}
	return strings.ToLower(string(n))
}

// passiveApplier is a custom applier that is used only during EVAL calls.
type passiveApplier struct {
	log finn.Logger
}

func (a *passiveApplier) Apply(
	conn redcon.Conn, cmd redcon.Command,
	mutate func() (interface{}, error),
	respond func(interface{}) (interface{}, error),
) (interface{}, error) {
	var v interface{}
	var err error
	if mutate != nil {
		if v, err = mutate(); err != nil {
			return nil, err
		}
	}
	if respond != nil {
		if v, err = respond(v); err != nil {
			return nil, err
		}
	}
	return v, err
}

func (a *passiveApplier) Log() finn.Logger {
	return a.log
}

// scriptConn is a custom redcon.Conn type that is only used
// during EVAL calls.
type passiveConn struct {
	resps []interface{}
}

func (conn *passiveConn) RemoteAddr() string             { return "" }
func (conn *passiveConn) Close() error                   { return nil }
func (conn *passiveConn) Context() interface{}           { return nil }
func (conn *passiveConn) SetContext(v interface{})       {}
func (conn *passiveConn) SetReadBuffer(bytes int)        {}
func (conn *passiveConn) Detach() redcon.DetachedConn    { return nil }
func (conn *passiveConn) ReadPipeline() []redcon.Command { return nil }
func (conn *passiveConn) PeekPipeline() []redcon.Command { return nil }

func (conn *passiveConn) WriteError(msg string) {
	conn.resps = append(conn.resps, errors.New(msg))
}
func (conn *passiveConn) WriteString(str string) {
	conn.resps = append(conn.resps, str)
}
func (conn *passiveConn) WriteBulk(bulk []byte) {
	conn.resps = append(conn.resps, bulk)
}
func (conn *passiveConn) WriteBulkString(bulk string) {
	conn.resps = append(conn.resps, []byte(bulk))
}
func (conn *passiveConn) WriteInt(num int) {
	conn.resps = append(conn.resps, int64(num))
}
func (conn *passiveConn) WriteInt64(num int64) {
	conn.resps = append(conn.resps, num)
}
func (conn *passiveConn) WriteArray(count int) {
	conn.resps = append(conn.resps, []int{count})
}
func (conn *passiveConn) WriteNull() {
	conn.resps = append(conn.resps, nil)
}
func (conn *passiveConn) WriteRaw(data []byte) {
	panic("WriteRaw is not allowed in a script context")
}
