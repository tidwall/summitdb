// Package finn provide a fast and simple Raft implementation.
package finn

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/tidwall/raft-boltdb"
	"github.com/tidwall/raft-fastlog"
	"github.com/tidwall/raft-redcon"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog"
)

var (
	// ErrUnknownCommand is returned when the command is not known.
	ErrUnknownCommand = errors.New("unknown command")
	// ErrWrongNumberOfArguments is returned when the number of arguments is wrong.
	ErrWrongNumberOfArguments = errors.New("wrong number of arguments")
	// ErrDisabled is returned when a feature is disabled.
	ErrDisabled = errors.New("disabled")
)

var (
	errInvalidCommand          = errors.New("invalid command")
	errInvalidConsistencyLevel = errors.New("invalid consistency level")
	errSyntaxError             = errors.New("syntax error")
	errInvalidResponse         = errors.New("invalid response")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

// Level is for defining the raft consistency level.
type Level int

// String returns a string representation of Level.
func (l Level) String() string {
	switch l {
	default:
		return "unknown"
	case Low:
		return "low"
	case Medium:
		return "medium"
	case High:
		return "high"
	}
}

const (
	// Low is "low" consistency. All readonly commands will can processed by
	// any node. Very fast but may have stale reads.
	Low Level = -1
	// Medium is "medium" consistency. All readonly commands can only be
	// processed by the leader. The command is not processed through the
	// raft log, therefore a very small (microseconds) chance for a stale
	// read is possible when a leader change occurs. Fast but only the leader
	// handles all reads and writes.
	Medium Level = 0
	// High is "high" consistency. All commands go through the raft log.
	// Not as fast because all commands must pass through the raft log.
	High Level = 1
)

// Backend is a raft log database type.
type Backend int

const (
	// FastLog is a persistent in-memory raft log.
	// This is the default.
	FastLog Backend = 0
	// Bolt is a persistent disk raft log.
	Bolt Backend = 1
	// InMem is a non-persistent in-memory raft log.
	InMem Backend = 2
)

// String returns a string representation of the Backend
func (b Backend) String() string {
	switch b {
	default:
		return "unknown"
	case FastLog:
		return "fastlog"
	case Bolt:
		return "bolt"
	case InMem:
		return "inmem"
	}
}

// LogLevel is used to define the verbosity of the log outputs
type LogLevel int

const (
	// Debug prints everything
	Debug LogLevel = -2
	// Verbose prints extra detail
	Verbose LogLevel = -1
	// Notice is the standard level
	Notice LogLevel = 0
	// Warning only prints warnings
	Warning LogLevel = 1
)

// Options are used to provide a Node with optional functionality.
type Options struct {
	// Consistency is the raft consistency level for reads.
	// Default is Medium
	Consistency Level
	// Durability is the fsync durability for disk writes.
	// Default is Medium
	Durability Level
	// Backend is the database backend.
	// Default is MemLog
	Backend Backend
	// LogLevel is the log verbosity
	// Default is Notice
	LogLevel LogLevel
	// LogOutput is the log writer
	// Default is os.Stderr
	LogOutput io.Writer
	// Accept is an optional function that can be used to
	// accept or deny a connection. It fires when new client
	// connections are created.
	// Return false to deny the connection.
	ConnAccept func(redcon.Conn) bool
	// ConnClosed is an optional function that fires
	// when client connections are closed.
	// If there was a network error, then the error will be
	// passed in as an argument.
	ConnClosed func(redcon.Conn, error)
}

// fillOptions fills in default options
func fillOptions(opts *Options) *Options {
	if opts == nil {
		opts = &Options{}
	}
	// copy and reassign the options
	nopts := *opts
	if nopts.LogOutput == nil {
		nopts.LogOutput = os.Stderr
	}
	return &nopts
}

// Logger is a logger
type Logger interface {
	// Printf write notice messages
	Printf(format string, args ...interface{})
	// Verbosef writes verbose messages
	Verbosef(format string, args ...interface{})
	// Noticef writes notice messages
	Noticef(format string, args ...interface{})
	// Warningf write warning messages
	Warningf(format string, args ...interface{})
	// Debugf writes debug messages
	Debugf(format string, args ...interface{})
}

// Applier is used to apply raft commands.
type Applier interface {
	// Apply applies a command
	Apply(conn redcon.Conn, cmd redcon.Command,
		mutate func() (interface{}, error),
		respond func(interface{}) (interface{}, error),
	) (interface{}, error)
	Log() Logger
}

// Machine handles raft commands and raft snapshotting.
type Machine interface {
	// Command is called by the Node for incoming commands.
	Command(a Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error)
	// Restore is used to restore data from a snapshot.
	Restore(rd io.Reader) error
	// Snapshot is used to support log compaction. This call should write a
	// snapshot to the provided writer.
	Snapshot(wr io.Writer) error
}

// Node represents a Raft server node.
type Node struct {
	mu       sync.RWMutex
	addr     string
	snapshot raft.SnapshotStore
	trans    *raftredcon.RedconTransport
	raft     *raft.Raft
	log      *redlog.Logger // the node logger
	mlog     *redlog.Logger // the machine logger
	closed   bool
	opts     *Options
	level    Level
	handler  Machine
	store    bigStore
}

// bigStore represents a raft store that conforms to
// raft.PeerStore, raft.LogStore, and raft.StableStore.
type bigStore interface {
	Close() error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(idx uint64, log *raft.Log) error
	StoreLog(log *raft.Log) error
	StoreLogs(logs []*raft.Log) error
	DeleteRange(min, max uint64) error
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
	Peers() ([]string, error)
	SetPeers(peers []string) error
}

// Open opens a Raft node and returns the Node to the caller.
func Open(dir, addr, join string, handler Machine, opts *Options) (node *Node, err error) {
	opts = fillOptions(opts)
	log := redlog.New(opts.LogOutput).Sub('N')
	log.SetFilter(redlog.HashicorpRaftFilter)
	log.SetIgnoreDups(true)
	switch opts.LogLevel {
	case Debug:
		log.SetLevel(0)
	case Verbose:
		log.SetLevel(1)
	case Notice:
		log.SetLevel(2)
	case Warning:
		log.SetLevel(3)
	}

	// if this function fails then write the error to the logger
	defer func() {
		if err != nil {
			log.Warningf("%v", err)
		}
	}()

	// create the directory
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	// create a node and assign it some fields
	n := &Node{
		log:     log,
		mlog:    log.Sub('C'),
		opts:    opts,
		level:   opts.Consistency,
		handler: handler,
	}

	var store bigStore
	if opts.Backend == Bolt {
		opts.Durability = High
		store, err = raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
		if err != nil {
			return nil, err
		}
	} else if opts.Backend == InMem {
		opts.Durability = Low
		store, err = raftfastlog.NewFastLogStore(":memory:", raftfastlog.Low, n.log.Sub('S'))
		if err != nil {
			return nil, err
		}
	} else {
		opts.Backend = FastLog
		var dur raftfastlog.Level
		switch opts.Durability {
		default:
			dur = raftfastlog.Medium
			opts.Durability = Medium
		case High:
			dur = raftfastlog.High
		case Low:
			dur = raftfastlog.Low
		}
		store, err = raftfastlog.NewFastLogStore(filepath.Join(dir, "raft.db"), dur, n.log.Sub('S'))
		if err != nil {
			return nil, err
		}
	}
	n.store = store

	n.log.Debugf("Consistency: %s, Durability: %s, Backend: %s", opts.Consistency, opts.Durability, opts.Backend)

	// get the peer list
	peers, err := n.store.Peers()
	if err != nil {
		n.Close()
		return nil, err
	}

	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LogOutput = n.log

	// Allow the node to enter single-mode, potentially electing itself, if
	// explicitly enabled and there is only 1 node in the cluster already.
	if join == "" && len(peers) <= 1 {
		n.log.Noticef("Enable single node")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}

	// create the snapshot store. This allows the Raft to truncate the log.
	n.snapshot, err = raft.NewFileSnapshotStore(dir, retainSnapshotCount, n.log)
	if err != nil {
		n.Close()
		return nil, err
	}

	// verify the syntax of the address.
	taddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		n.Close()
		return nil, err
	}

	// Set the atomic flag which indicates that we can accept Redcon commands.
	var doReady uint64

	// start the raft server
	n.addr = taddr.String()
	n.trans, err = raftredcon.NewRedconTransport(
		n.addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			if atomic.LoadUint64(&doReady) != 0 {
				n.doCommand(conn, cmd)
			} else {
				conn.WriteError("ERR raft not ready")
			}
		}, opts.ConnAccept, opts.ConnClosed,
		n.log.Sub('L'),
	)
	if err != nil {
		n.Close()
		return nil, err
	}

	// Instantiate the Raft systems.
	n.raft, err = raft.NewRaft(config, (*nodeFSM)(n),
		n.store, n.store, n.snapshot, n.store, n.trans)
	if err != nil {
		n.Close()
		return nil, err
	}

	// set the atomic flag which indicates that we can accept Redcon commands.
	atomic.AddUint64(&doReady, 1)

	// if --join was specified, make the join request.
	for {
		if join != "" && len(peers) == 0 {
			if err := reqRaftJoin(join, n.addr); err != nil {
				if strings.HasPrefix(err.Error(), "TRY ") {
					// we received a "TRY addr" response. let forward the join to
					// the specified address"
					join = strings.Split(err.Error(), " ")[1]
					continue
				}
				return nil, fmt.Errorf("failed to join node at %v: %v", join, err)
			}
		}
		break
	}
	return n, nil
}

// Close closes the node
func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// shutdown the raft, but do not handle the future error. :PPA:
	if n.raft != nil {
		n.raft.Shutdown().Error()
	}
	if n.trans != nil {
		n.trans.Close()
	}
	// close the raft database
	if n.store != nil {
		n.store.Close()
	}
	n.closed = true
	return nil
}

// Log returns the active logger for printing messages
func (n *Node) Log() Logger {
	return n.mlog
}

// leader returns the client address for the leader
func (n *Node) leader() string {
	return n.raft.Leader()
}

// reqRaftJoin does a remote "RAFTJOIN" command at the specified address.
func reqRaftJoin(join, raftAddr string) error {
	resp, _, err := raftredcon.Do(join, nil, []byte("raftaddpeer"), []byte(raftAddr))
	if err != nil {
		return err
	}
	if string(resp) != "OK" {
		return errors.New("invalid response")
	}
	return nil
}

// scanForErrors returns pipeline errors. All messages must be errors
func scanForErrors(buf []byte) [][]byte {
	var res [][]byte
	for len(buf) > 0 {
		if buf[0] != '-' {
			return nil
		}
		buf = buf[1:]
		for i := 0; i < len(buf); i++ {
			if buf[i] == '\n' && i > 0 && buf[i-1] == '\r' {
				res = append(res, buf[:i-1])
				buf = buf[i+1:]
				break
			}
		}
	}
	return res
}

func (n *Node) translateError(err error, cmd string) string {
	if err.Error() == ErrDisabled.Error() || err.Error() == ErrUnknownCommand.Error() {
		return "ERR unknown command '" + cmd + "'"
	} else if err.Error() == ErrWrongNumberOfArguments.Error() {
		return "ERR wrong number of arguments for '" + cmd + "' command"
	} else if err.Error() == raft.ErrNotLeader.Error() {
		leader := n.raft.Leader()
		if leader == "" {
			return "ERR leader not known"
		}
		return "TRY " + leader
	}
	return strings.TrimSpace(strings.Split(err.Error(), "\n")[0])
}

// doCommand executes a client command which is processed through the raft pipeline.
func (n *Node) doCommand(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) == 0 {
		return nil, nil
	}
	var val interface{}
	var err error
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		val, err = n.handler.Command((*nodeApplier)(n), conn, cmd)
		if err == ErrDisabled {
			err = ErrUnknownCommand
		}
	case "raftaddpeer":
		val, err = n.doRaftAddPeer(conn, cmd)
	case "raftremovepeer":
		val, err = n.doRaftRemovePeer(conn, cmd)
	case "raftleader":
		val, err = n.doRaftLeader(conn, cmd)
	case "raftsnapshot":
		val, err = n.doRaftSnapshot(conn, cmd)
	case "raftshrinklog":
		val, err = n.doRaftShrinkLog(conn, cmd)
	case "raftstate":
		val, err = n.doRaftState(conn, cmd)
	case "raftstats":
		val, err = n.doRaftStats(conn, cmd)
	case "quit":
		val, err = n.doQuit(conn, cmd)
	case "ping":
		val, err = n.doPing(conn, cmd)
	}
	if err != nil && conn != nil {
		// it's possible that this was a pipelined response.
		wr := redcon.BaseWriter(conn)
		if wr != nil {
			buf := wr.Buffer()
			rerrs := scanForErrors(buf)
			if len(rerrs) > 0 {
				wr.SetBuffer(nil)
				for _, rerr := range rerrs {
					conn.WriteError(n.translateError(errors.New(string(rerr)), string(cmd.Args[0])))
				}
			}
		}
		conn.WriteError(n.translateError(err, string(cmd.Args[0])))
	}
	return val, err
}

// doPing handles a "PING" client command.
func (n *Node) doPing(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	switch len(cmd.Args) {
	default:
		return nil, ErrWrongNumberOfArguments
	case 1:
		conn.WriteString("PONG")
	case 2:
		conn.WriteBulk(cmd.Args[1])
	}
	return nil, nil
}

// doRaftLeader handles a "RAFTLEADER" client command.
func (n *Node) doRaftLeader(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, ErrWrongNumberOfArguments
	}
	conn.WriteBulkString(n.raft.Leader())
	return nil, nil
}

// doRaftSnapshot handles a "RAFTSNAPSHOT" client command.
func (n *Node) doRaftSnapshot(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, ErrWrongNumberOfArguments
	}
	f := n.raft.Snapshot()
	err := f.Error()
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return nil, nil
	}
	conn.WriteString("OK")
	return nil, nil
}

type shrinkable interface {
	Shrink() error
}

// doRaftShrinkLog handles a "RAFTSHRINKLOG" client command.
func (n *Node) doRaftShrinkLog(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, ErrWrongNumberOfArguments
	}
	if s, ok := n.store.(shrinkable); ok {
		err := s.Shrink()
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return nil, nil
		}
		conn.WriteString("OK")
		return nil, nil
	}
	conn.WriteError("ERR log is not shrinkable")
	return nil, nil
}

// doRaftState handles a "RAFTSTATE" client command.
func (n *Node) doRaftState(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, ErrWrongNumberOfArguments
	}
	conn.WriteBulkString(n.raft.State().String())
	return nil, nil
}

// doRaftStatus handles a "RAFTSTATUS" client command.
func (n *Node) doRaftStats(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 1 {
		return nil, ErrWrongNumberOfArguments
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	stats := n.raft.Stats()
	keys := make([]string, 0, len(stats))
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	conn.WriteArray(len(keys) * 2)
	for _, key := range keys {
		conn.WriteBulkString(key)
		conn.WriteBulkString(stats[key])
	}
	return nil, nil
}

// doQuit handles a "QUIT" client command.
func (n *Node) doQuit(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	conn.WriteString("OK")
	conn.Close()
	return nil, nil
}

// doRaftAddPeer handles a "RAFTADDPEER address" client command.
func (n *Node) doRaftAddPeer(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, ErrWrongNumberOfArguments
	}
	n.log.Noticef("Received add peer request from %v", string(cmd.Args[1]))
	f := n.raft.AddPeer(string(cmd.Args[1]))
	if f.Error() != nil {
		return nil, f.Error()
	}
	n.log.Noticef("Node %v added successfully", string(cmd.Args[1]))
	conn.WriteString("OK")
	return nil, nil
}

// doRaftRemovePeer handles a "RAFTREMOVEPEER address" client command.
func (n *Node) doRaftRemovePeer(conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	if len(cmd.Args) != 2 {
		return nil, ErrWrongNumberOfArguments
	}
	n.log.Noticef("Received remove peer request from %v", string(cmd.Args[1]))
	f := n.raft.RemovePeer(string(cmd.Args[1]))
	if f.Error() != nil {
		return nil, f.Error()
	}
	n.log.Noticef("Node %v detached successfully", string(cmd.Args[1]))
	conn.WriteString("OK")
	return nil, nil
}

// raftApplyCommand encodes a series of args into a raft command and
// applies it to the index.
func (n *Node) raftApplyCommand(cmd redcon.Command) (interface{}, error) {
	f := n.raft.Apply(cmd.Raw, raftTimeout)
	if err := f.Error(); err != nil {
		return nil, err
	}
	// we check for the response to be an error and return it as such.
	switch v := f.Response().(type) {
	default:
		return v, nil
	case error:
		return nil, v
	}
}

// raftLevelGuard is used to process readonly commands depending on the
// consistency readonly level.
// It either:
// - low consistency: just processes the command without concern about
//   leadership or cluster state.
// - medium consistency: makes sure that the node is the leader first.
// - high consistency: sends a blank command through the raft pipeline to
// ensure that the node is thel leader, the raft index is incremented, and
// that the cluster is sane before processing the readonly command.
func (n *Node) raftLevelGuard() error {
	switch n.level {
	default:
		// a valid level is required
		return errInvalidConsistencyLevel
	case Low:
		// anything goes.
		return nil
	case Medium:
		// must be the leader
		if n.raft.State() != raft.Leader {
			return raft.ErrNotLeader
		}
		return nil
	case High:
		// process a blank command. this will update the raft log index
		// and allow for readonly commands to process in order without
		// serializing the actual command.
		f := n.raft.Apply(nil, raftTimeout)
		if err := f.Error(); err != nil {
			return err
		}
		// the blank command succeeded.
		v := f.Response()
		// check if response was an error and return that.
		switch v := v.(type) {
		case nil:
			return nil
		case error:
			return v
		}
		return errInvalidResponse
	}
}

// nodeApplier exposes the Applier interface of the Node type
type nodeApplier Node

// Apply executes a command through raft.
// The mutate param should be set to nil for readonly commands.
// The repsond param is required and any response to conn happens here.
// The return value from mutate will be passed into the respond param.
func (m *nodeApplier) Apply(
	conn redcon.Conn,
	cmd redcon.Command,
	mutate func() (interface{}, error),
	respond func(interface{}) (interface{}, error),
) (interface{}, error) {
	var val interface{}
	var err error
	if mutate == nil {
		// no apply, just do a level guard.
		if err := (*Node)(m).raftLevelGuard(); err != nil {
			return nil, err
		}
	} else if conn == nil {
		// this is happening on a follower node.
		return mutate()
	} else {
		// this is happening on the leader node.
		// apply the command to the raft log.
		val, err = (*Node)(m).raftApplyCommand(cmd)
	}
	if err != nil {
		return nil, err
	}
	// responde
	return respond(val)
}

// Log returns the active logger for printing messages
func (m *nodeApplier) Log() Logger {
	return (*Node)(m).Log()
}

// nodeFSM exposes the raft.FSM interface of the Node type
type nodeFSM Node

// Apply applies a Raft log entry to the key-value store.
func (m *nodeFSM) Apply(l *raft.Log) interface{} {
	if len(l.Data) == 0 {
		// blank data
		return nil
	}
	cmd, err := redcon.Parse(l.Data)
	if err != nil {
		return err
	}
	val, err := (*Node)(m).doCommand(nil, cmd)
	if err != nil {
		return err
	}
	return val
}

// Restore stores the key-value store to a previous state.
func (m *nodeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return (*Node)(m).handler.Restore(rc)
}

// Persist writes the snapshot to the given sink.
func (m *nodeFSM) Persist(sink raft.SnapshotSink) error {
	if err := (*Node)(m).handler.Snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

// Release deletes the temp file
func (m *nodeFSM) Release() {}

// Snapshot returns a snapshot of the key-value store.
func (m *nodeFSM) Snapshot() (raft.FSMSnapshot, error) {
	return m, nil
}
