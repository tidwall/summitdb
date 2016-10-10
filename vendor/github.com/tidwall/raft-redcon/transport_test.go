package raftredcon

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/tidwall/redcon"
)

const addr = ":17831"

func TestServer(t *testing.T) {
	s, err := NewRedconTransport(addr, passthroughHandler, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	go func() {
		for rpc := range s.consumer {
			var resp interface{}
			var err error
			switch args := rpc.Command.(type) {
			case *raft.RequestVoteRequest:
				if string(args.Candidate) != "candidate" {
					err = fmt.Errorf("expecting 'candidate', got '%v'", string(args.Candidate))
				} else if args.LastLogIndex != 1 {
					err = fmt.Errorf("expecting '1', got '%v'", args.LastLogIndex)
				} else if args.LastLogTerm != 2 {
					err = fmt.Errorf("expecting '2', got '%v'", args.LastLogTerm)
				} else if args.Term != 3 {
					err = fmt.Errorf("expecting '3', got '%v'", args.Term)
				} else {
					resp = &raft.RequestVoteResponse{
						Granted: true,
						Peers:   []byte("peers"),
						Term:    4,
					}
				}
			case *raft.AppendEntriesRequest:
				if len(args.Entries) != 2 {
					err = fmt.Errorf("expecting '2', got '%v'", len(args.Entries))
				} else if string(args.Entries[0].Data) != "log1" || args.Entries[0].Index != 1 || args.Entries[0].Term != 2 || args.Entries[0].Type != raft.LogAddPeer {
					err = fmt.Errorf("invalid log entry 1")
				} else if string(args.Entries[1].Data) != "log2" || args.Entries[1].Index != 3 || args.Entries[1].Term != 4 || args.Entries[1].Type != raft.LogRemovePeer {
					err = fmt.Errorf("invalid log entry 2")
				} else if string(args.Leader) != "leader" {
					err = fmt.Errorf("expecting 'leader', got '%v'", string(args.Leader))
				} else if args.LeaderCommitIndex != 5 {
					err = fmt.Errorf("expecting '5', got '%v'", args.LeaderCommitIndex)
				} else if args.PrevLogEntry != 6 {
					err = fmt.Errorf("expecting '6', got '%v'", args.PrevLogEntry)
				} else if args.PrevLogTerm != 7 {
					err = fmt.Errorf("expecting '7', got '%v'", args.PrevLogTerm)
				} else if args.Term != 8 {
					err = fmt.Errorf("expecting '8', got '%v'", args.Term)
				} else {
					resp = &raft.AppendEntriesResponse{
						LastLog:        9,
						NoRetryBackoff: true,
						Success:        true,
						Term:           10,
					}
				}
			case *raft.InstallSnapshotRequest:
				if args.LastLogIndex != 1 {
					err = fmt.Errorf("expecting '1', got '%v'", args.LastLogIndex)
				} else if args.LastLogTerm != 2 {
					err = fmt.Errorf("expecting '2', got '%v'", args.LastLogTerm)
				} else if string(args.Leader) != "leader" {
					err = fmt.Errorf("expecting 'leader', got '%v'", string(args.Leader))
				} else if string(args.Peers) != "peers" {
					err = fmt.Errorf("expecting 'peers', got '%v'", string(args.Peers))
				} else if args.Size != 3 {
					err = fmt.Errorf("expecting '3', got '%v'", args.Size)
				} else if args.Term != 4 {
					err = fmt.Errorf("expecting '4', got '%v'", args.Term)
				} else {
					var data []byte
					data, err = ioutil.ReadAll(rpc.Reader)
					if err == nil {
						if string(data) != "look ma, i've gots the data!" {
							err = fmt.Errorf("expecting 'look ma, i've gots the data!', got '%v'", string(data))
						} else {
							resp = &raft.InstallSnapshotResponse{
								Success: true,
								Term:    5,
							}
						}
					}
				}
			}
			rpc.RespChan <- raft.RPCResponse{Response: resp, Error: err}
		}
	}()
	t.Run("AppendEntries", func(t *testing.T) { SubAppendEntries(t, s) })
	t.Run("RequestVote", func(t *testing.T) { SubRequestVote(t, s) })
	t.Run("InstallSnapshot", func(t *testing.T) { SubInstallSnapshot(t, s) })
	t.Run("Passthrough", func(t *testing.T) { SubPassthrough(t, s) })

}
func SubInstallSnapshot(t *testing.T, s *RedconTransport) {
	var args raft.InstallSnapshotRequest
	var resp raft.InstallSnapshotResponse
	args.LastLogIndex = 1
	args.LastLogTerm = 2
	args.Leader = []byte("leader")
	args.Peers = []byte("peers")
	args.Size = 3
	args.Term = 4
	if err := s.InstallSnapshot(addr, &args, &resp, bytes.NewBufferString("look ma, i've gots the data!")); err != nil {
		t.Fatal(err)
	}
	if resp.Success != true {
		t.Fatalf("expected 'true', got '%v'", resp.Success)
	}
	if resp.Term != 5 {
		t.Fatalf("expected '5', got '%v'", resp.Term)
	}
}
func SubAppendEntries(t *testing.T, s *RedconTransport) {
	var args raft.AppendEntriesRequest
	var resp raft.AppendEntriesResponse
	args.Entries = []*raft.Log{
		&raft.Log{Data: []byte("log1"), Index: 1, Term: 2, Type: raft.LogAddPeer},
		&raft.Log{Data: []byte("log2"), Index: 3, Term: 4, Type: raft.LogRemovePeer},
	}
	args.Leader = []byte("leader")
	args.LeaderCommitIndex = 5
	args.PrevLogEntry = 6
	args.PrevLogTerm = 7
	args.Term = 8
	if err := s.AppendEntries(addr, &args, &resp); err != nil {
		t.Fatal(err)
	}
}
func SubRequestVote(t *testing.T, s *RedconTransport) {
	var args raft.RequestVoteRequest
	var resp raft.RequestVoteResponse
	args.Candidate = []byte("candidate")
	args.LastLogIndex = 1
	args.LastLogTerm = 2
	args.Term = 3
	if err := s.RequestVote(addr, &args, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Granted != true {
		t.Fatalf("expected 'true', got '%v'", resp.Granted)
	}
	if string(resp.Peers) != "peers" {
		t.Fatalf("expected 'peers', got '%v'", string(resp.Peers))
	}
	if resp.Term != 4 {
		t.Fatalf("expected '4', got '%v'", resp.Term)
	}
}
func SubPassthrough(t *testing.T, s *RedconTransport) {
	resp, _, err := Do(addr, nil, []byte("PING"), []byte("say hi to the wicker people"))
	if err != nil {
		t.Fatal(err)
	}
	if string(resp) != "say hi to the wicker people" {
		t.Fatalf("expecting 'say hi to the wicker people', got '%v'", string(resp))
	}
}

func passthroughHandler(conn redcon.Conn, cmd redcon.Command) {
	if string(cmd.Args[0]) != "PING" {
		conn.WriteError("ERR " + fmt.Sprintf("expecting 'PING', got '%v'", string(cmd.Args[0])))
	} else if len(cmd.Args) != 2 {
		conn.WriteError("ERR " + fmt.Sprintf("expecting '2', got '%v'", len(cmd.Args)))
	} else if string(cmd.Args[1]) != "say hi to the wicker people" {
		conn.WriteError("ERR " + fmt.Sprintf("expecting 'say hi to the wicker people', got '%v'", string(cmd.Args[1])))
	} else {
		conn.WriteBulk(cmd.Args[1])
	}
}
