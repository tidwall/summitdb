// Package raftredcon provides a raft transport using the Redis protocol.
package raftredcon

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/hashicorp/raft"
	"github.com/tidwall/redcon"
)

var (
	errInvalidNumberOfArgs = errors.New("invalid number or arguments")
	errInvalidCommand      = errors.New("invalid command")
	errInvalidResponse     = errors.New("invalid response")
)

type RedconTransport struct {
	addr     string
	consumer chan raft.RPC
	handleFn func(conn redcon.Conn, cmd redcon.Command)
	server   *redcon.Server

	mu     sync.Mutex
	pools  map[string]*redis.Pool
	closed bool
	log    io.Writer
}

func NewRedconTransport(
	bindAddr string,
	handle func(conn redcon.Conn, cmd redcon.Command),
	accept func(conn redcon.Conn) bool,
	closed func(conn redcon.Conn, err error),
	logOutput io.Writer,
) (*RedconTransport, error) {
	t := &RedconTransport{
		addr:     bindAddr,
		consumer: make(chan raft.RPC),
		handleFn: handle,
		pools:    make(map[string]*redis.Pool),
		log:      logOutput,
	}
	t.server = redcon.NewServer(bindAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			t.handle(conn, cmd)
		}, accept, closed)
	signal := make(chan error)
	go t.server.ListenServeAndSignal(signal)
	err := <-signal
	if err != nil {
		return nil, err
	}
	return t, nil
}

// newTargetPool returns a Redigo pool for the specified target node.
func newTargetPool(target string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     5,           // figure 5 should suffice most clusters.
		IdleTimeout: time.Minute, //
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", target)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

// Close is used to permanently disable the transport
func (t *RedconTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("closed")
	}
	t.closed = true
	t.server.Close()
	for _, pool := range t.pools {
		pool.Close()
	}
	t.pools = nil
	return nil
}

// getPool returns a usable pool for obtaining a connection to the specified target.
func (t *RedconTransport) getPool(target string) (*redis.Pool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, errors.New("closed")
	}
	pool, ok := t.pools[target]
	if !ok {
		pool = newTargetPool(target)
		t.pools[target] = pool
	}
	return pool, nil
}

// getConn returns a connection to the target.
func (t *RedconTransport) getConn(target string) (redis.Conn, error) {
	pool, err := t.getPool(target)
	if err != nil {
		return nil, err
	}
	return pool.Get(), nil
}

// AppendEntriesPipeline returns an interface that can be used to pipeline AppendEntries requests.
func (t *RedconTransport) AppendEntriesPipeline(target string) (raft.AppendPipeline, error) {
	return nil, raft.ErrPipelineReplicationNotSupported
}

// encodeAppendEntriesRequest encodes AppendEntriesRequest arguments into a
// tight binary format.
func encodeAppendEntriesRequest(args *raft.AppendEntriesRequest) []byte {
	n := make([]byte, 8)       // used to store uint64s
	b := make([]byte, 40, 256) // encoded message goes here
	binary.LittleEndian.PutUint64(b[0:8], args.Term)
	binary.LittleEndian.PutUint64(b[8:16], args.PrevLogEntry)
	binary.LittleEndian.PutUint64(b[16:24], args.PrevLogTerm)
	binary.LittleEndian.PutUint64(b[24:32], args.LeaderCommitIndex)
	binary.LittleEndian.PutUint64(b[32:40], uint64(len(args.Leader)))
	b = append(b, args.Leader...)
	binary.LittleEndian.PutUint64(n, uint64(len(args.Entries)))
	b = append(b, n...)
	for _, entry := range args.Entries {
		binary.LittleEndian.PutUint64(n, entry.Index)
		b = append(b, n...)
		binary.LittleEndian.PutUint64(n, entry.Term)
		b = append(b, n...)
		b = append(b, byte(entry.Type))
		binary.LittleEndian.PutUint64(n, uint64(len(entry.Data)))
		b = append(b, n...)
		b = append(b, entry.Data...)
	}
	return b
}

// decodeAppendEntriesRequest decodes AppendEntriesRequest data.
// Returns true when successful
func decodeAppendEntriesRequest(b []byte, args *raft.AppendEntriesRequest) bool {
	if len(b) < 40 {
		return false
	}
	args.Term = binary.LittleEndian.Uint64(b[0:8])
	args.PrevLogEntry = binary.LittleEndian.Uint64(b[8:16])
	args.PrevLogTerm = binary.LittleEndian.Uint64(b[16:24])
	args.LeaderCommitIndex = binary.LittleEndian.Uint64(b[24:32])
	args.Leader = make([]byte, int(binary.LittleEndian.Uint64(b[32:40])))
	b = b[40:]
	if len(b) < len(args.Leader) {
		return false
	}
	copy(args.Leader, b[:len(args.Leader)])
	b = b[len(args.Leader):]
	if len(b) < 8 {
		return false
	}
	args.Entries = make([]*raft.Log, int(binary.LittleEndian.Uint64(b)))
	b = b[8:]
	for i := 0; i < len(args.Entries); i++ {
		if len(b) < 25 {
			return false
		}
		args.Entries[i] = &raft.Log{}
		args.Entries[i].Index = binary.LittleEndian.Uint64(b[0:8])
		args.Entries[i].Term = binary.LittleEndian.Uint64(b[8:16])
		args.Entries[i].Type = raft.LogType(b[16])
		args.Entries[i].Data = make([]byte, int(binary.LittleEndian.Uint64(b[17:25])))
		b = b[25:]
		if len(b) < len(args.Entries[i].Data) {
			return false
		}
		copy(args.Entries[i].Data, b[:len(args.Entries[i].Data)])
		b = b[len(args.Entries[i].Data):]
	}
	return len(b) == 0
}

func encodeAppendEntriesResponse(args *raft.AppendEntriesResponse) []byte {
	b := make([]byte, 18)
	binary.LittleEndian.PutUint64(b[0:8], args.Term)
	binary.LittleEndian.PutUint64(b[8:16], args.LastLog)
	if args.Success {
		b[16] = 1
	}
	if args.NoRetryBackoff {
		b[17] = 1
	}
	return b
}

func decodeAppendEntriesResponse(b []byte, args *raft.AppendEntriesResponse) bool {
	if len(b) != 18 {
		return false
	}
	args.Term = binary.LittleEndian.Uint64(b[0:8])
	args.LastLog = binary.LittleEndian.Uint64(b[8:16])
	if b[16] == 1 {
		args.Success = true
	} else {
		args.Success = false
	}
	if b[17] == 1 {
		args.NoRetryBackoff = true
	} else {
		args.NoRetryBackoff = false
	}
	return true
}

// AppendEntries implements the Transport interface.
func (t *RedconTransport) AppendEntries(target string, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	conn, err := t.getConn(target)
	if err != nil {
		return err
	}
	defer conn.Close()

	reply, err := conn.Do("raftappendentries", encodeAppendEntriesRequest(args))
	if err != nil {
		return err
	}
	switch val := reply.(type) {
	default:
		return errors.New("invalid response")
	case redis.Error:
		return val
	case []byte:
		if !decodeAppendEntriesResponse(val, resp) {
			return errors.New("invalid response")
		}
		return nil
	}
}

func (t *RedconTransport) handleAppendEntries(cmd redcon.Command) ([]byte, error) {
	if len(cmd.Args) != 2 {
		return nil, errInvalidNumberOfArgs
	}
	var rpc raft.RPC
	var aer raft.AppendEntriesRequest
	if !decodeAppendEntriesRequest(cmd.Args[1], &aer) {
		return nil, errors.New("invalid request")
	}
	rpc.Command = &aer
	respChan := make(chan raft.RPCResponse)
	rpc.RespChan = respChan
	t.consumer <- rpc
	rresp := <-respChan
	if rresp.Error != nil {
		return nil, rresp.Error
	}
	resp, ok := rresp.Response.(*raft.AppendEntriesResponse)
	if !ok {
		return nil, errors.New("invalid response")
	}
	data := encodeAppendEntriesResponse(resp)
	return data, nil
}

// RequestVote implements the Transport interface.
func (t *RedconTransport) RequestVote(target string, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	data, _ := json.Marshal(args)
	val, _, err := Do(target, nil, []byte("raftrequestvote"), data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(val, resp); err != nil {
		return err
	}
	return nil
}

func (t *RedconTransport) handleRequestVote(cmd redcon.Command) ([]byte, error) {
	if len(cmd.Args) != 2 {
		return nil, errInvalidNumberOfArgs
	}
	var rpc raft.RPC
	var aer raft.RequestVoteRequest
	if err := json.Unmarshal(cmd.Args[1], &aer); err != nil {
		return nil, err
	}
	rpc.Command = &aer
	respChan := make(chan raft.RPCResponse)
	rpc.RespChan = respChan
	t.consumer <- rpc
	rresp := <-respChan
	if rresp.Error != nil {
		return nil, rresp.Error
	}
	resp, ok := rresp.Response.(*raft.RequestVoteResponse)
	if !ok {
		return nil, errors.New("invalid response")
	}
	data, _ := json.Marshal(resp)
	return data, nil
}

// InstallSnapshot implmenents the Transport interface.
func (t *RedconTransport) InstallSnapshot(
	target string, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader,
) error {
	// Use a dedicated connection for snapshots. This operation happens very infrequently, but when it does
	// it often passes a lot of data.
	conn, err := net.Dial("tcp", target)
	if err != nil {
		return err
	}
	defer conn.Close()
	rd := bufio.NewReader(conn)
	// use JSON encoded arguments for the initial request.
	rdata, err := json.Marshal(args)
	if err != nil {
		return err
	}
	// send RAFTINSTALLSNAPSHOT {args}
	if _, err := conn.Write(buildCommand(nil, []byte("raftinstallsnapshot"), rdata)); err != nil {
		return err
	}
	// receive +OK
	line, err := response(rd)
	if err != nil {
		return err
	}
	if string(line) != "OK" {
		return errInvalidResponse
	}
	var i int
	var cmd []byte                   // reuse buffer
	buf := make([]byte, 4*1024*1024) // 4MB chunk
	for {
		n, ferr := data.Read(buf)
		if n > 0 {
			// send CHUNK data
			cmd = buildCommand(cmd, []byte("chunk"), buf[:n])
			if _, err := conn.Write(cmd); err != nil {
				return err
			}
			cmd = cmd[:0] // set len to zero for reuse
			// receive +OK
			line, err := response(rd)
			if err != nil {
				return err
			}
			if string(line) != "OK" {
				return errInvalidResponse
			}
			i++
		}
		if ferr != nil {
			if ferr == io.EOF {
				break
			}
			return ferr
		}
	}
	// send DONE
	if _, err := conn.Write(buildCommand(nil, []byte("done"))); err != nil {
		return err
	}
	// receive ${resp}
	line, err = response(rd)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(line, resp); err != nil {
		return err
	}
	return nil
}

func (t *RedconTransport) handleInstallSnapshot(conn redcon.DetachedConn, arg []byte) {
	err := func() error {
		var rpc raft.RPC
		rpc.Command = &raft.InstallSnapshotRequest{}
		if err := json.Unmarshal(arg, &rpc.Command); err != nil {
			return err
		}
		conn.WriteString("OK")
		if err := conn.Flush(); err != nil {
			return err
		}
		rd, wr := io.Pipe()
		go func() {
			err := func() error {
				var i int
				for {
					cmd, err := conn.ReadCommand()
					if err != nil {
						return err
					}
					switch strings.ToLower(string(cmd.Args[0])) {
					default:
						return errInvalidCommand
					case "chunk":
						if len(cmd.Args) != 2 {
							return errInvalidNumberOfArgs
						}
						if _, err := wr.Write(cmd.Args[1]); err != nil {
							return err
						}
						conn.WriteString("OK")
						if err := conn.Flush(); err != nil {
							return err
						}
						i++
					case "done":
						return nil
					}
				}
			}()
			if err != nil {
				wr.CloseWithError(err)
			} else {
				wr.Close()
			}
		}()
		rpc.Reader = rd
		respChan := make(chan raft.RPCResponse)
		rpc.RespChan = respChan
		t.consumer <- rpc
		resp := <-respChan
		if resp.Error != nil {
			return resp.Error
		}
		data, err := json.Marshal(resp.Response)
		if err != nil {
			return err
		}
		conn.WriteBulk(data)
		if err := conn.Flush(); err != nil {
			return err
		}
		return nil
	}()
	if t.log != nil {
		if err != nil {
			fmt.Fprintf(t.log, "%s [WARN] transport: Handle snapshot failed: %v\n",
				time.Now().Format("2006/01/02 15:04:05"), err)
		} else {
			fmt.Fprintf(t.log, "%s [VERB] transport: Handle shapshot completed\n",
				time.Now().Format("2006/01/02 15:04:05"))
		}
	}
}

func (t *RedconTransport) handle(conn redcon.Conn, cmd redcon.Command) {
	var err error
	var res []byte
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		if t.handleFn != nil {
			t.handleFn(conn, cmd)
		} else {
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		}
		return
	case "raftinstallsnapshot":
		if len(cmd.Args) != 2 {
			err = errInvalidNumberOfArgs
		} else {
			// detach connection and forward to the background
			dconn := conn.Detach()
			go func() {
				defer dconn.Close()
				t.handleInstallSnapshot(dconn, cmd.Args[1])
			}()
			return
		}
	case "raftrequestvote":
		res, err = t.handleRequestVote(cmd)
	case "raftappendentries":
		res, err = t.handleAppendEntries(cmd)
	}
	if err != nil {
		if err == errInvalidNumberOfArgs {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		} else {
			conn.WriteError("ERR " + err.Error())
		}
	} else {
		conn.WriteBulk(res)
	}
}

// Consumer implmenents the Transport interface.
func (t *RedconTransport) Consumer() <-chan raft.RPC { return t.consumer }

// LocalAddr implmenents the Transport interface.
func (t *RedconTransport) LocalAddr() string { return t.addr }

// EncodePeer implmenents the Transport interface.
func (t *RedconTransport) EncodePeer(peer string) []byte { return []byte(peer) }

// DecodePeer implmenents the Transport interface.
func (t *RedconTransport) DecodePeer(peer []byte) string { return string(peer) }

// SetHeartbeatHandler implmenents the Transport interface.
func (t *RedconTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {}

// Do is a helper function that makes a very simple remote request with
// the specified command.
// The addr param is the target server address.
// The buf param is an optional reusable buffer, this can be nil.
// The args are the command arguments such as "SET", "key", "value".
// Return response is a bulk, string, or an error.
// The nbuf is a reuseable buffer, this can be ignored.
func Do(addr string, buf []byte, args ...[]byte) (resp []byte, nbuf []byte, err error) {
	cmd := buildCommand(buf, args...)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, cmd, err
	}
	defer conn.Close()
	if _, err = conn.Write(cmd); err != nil {
		return nil, cmd, err
	}
	resp, err = response(bufio.NewReader(conn))
	return resp, cmd[:0], err
}

func response(rd *bufio.Reader) ([]byte, error) {
	c, err := rd.ReadByte()
	if err != nil {
		return nil, err
	}
	switch c {
	default:
		return nil, errors.New("invalid response")
	case '+', '-', '$', ':', '*':
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 2 || line[len(line)-2] != '\r' {
			return nil, errors.New("invalid response")
		}
		line = line[:len(line)-2]
		switch c {
		default:
			return nil, errors.New("invalid response")
		case '*':
			n, err := strconv.ParseUint(string(line), 10, 64)
			if err != nil {
				return nil, err
			}
			var buf []byte
			for i := 0; i < int(n); i++ {
				res, err := response(rd)
				if err != nil {
					return nil, err
				}
				buf = append(buf, res...)
				buf = append(buf, "\n"...)
			}
			return buf, nil
		case '+', ':':
			return line, nil
		case '-':
			return nil, errors.New(string(line))
		case '$':
			n, err := strconv.ParseUint(string(line), 10, 64)
			if err != nil {
				return nil, err
			}
			data := make([]byte, int(n)+2)
			if _, err := io.ReadFull(rd, data); err != nil {
				return nil, err
			}
			if data[len(data)-2] != '\r' || data[len(data)-1] != '\n' {
				return nil, errors.New("invalid response")
			}
			return data[:len(data)-2], nil
		}
	}
}

// buildCommand builds a valid redis command and appends to buf.
// The return value is the newly appended buf.
func buildCommand(buf []byte, args ...[]byte) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(len(args)), 10)...)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = append(buf, strconv.FormatInt(int64(len(arg)), 10)...)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

func ReadRawResponse(rd *bufio.Reader) (raw []byte, kind byte, err error) {
	kind, err = rd.ReadByte()
	if err != nil {
		return raw, kind, err
	}
	raw = append(raw, kind)
	switch kind {
	default:
		return raw, kind, errors.New("invalid response")
	case '+', '-', '$', ':', '*':
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return raw, kind, err
		}
		raw = append(raw, line...)
		if len(line) < 2 || line[len(line)-2] != '\r' {
			return raw, kind, errors.New("invalid response")
		}
		line = line[:len(line)-2]
		switch kind {
		default:
			return raw, kind, errors.New("invalid response")
		case '+', ':', '-':
			return raw, kind, nil
		case '*':
			n, err := strconv.ParseInt(string(line), 10, 64)
			if err != nil {
				return raw, kind, err
			}
			if n > 0 {
				for i := 0; i < int(n); i++ {
					res, _, err := ReadRawResponse(rd)
					if err != nil {
						return raw, kind, err
					}
					raw = append(raw, res...)
				}
			}
		case '$':
			n, err := strconv.ParseInt(string(line), 10, 64)
			if err != nil {
				return raw, kind, err
			}
			if n > 0 {
				data := make([]byte, int(n)+2)
				if _, err := io.ReadFull(rd, data); err != nil {
					return raw, kind, err
				}
				if data[len(data)-2] != '\r' || data[len(data)-1] != '\n' {
					return raw, kind, errors.New("invalid response")
				}
				raw = append(raw, data...)
			}
		}
		return raw, kind, nil
	}
}
