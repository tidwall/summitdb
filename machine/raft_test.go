package machine

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

func subTestRaft(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "snapshot", raft_SNAPSHOT_test)
	runStep(t, mc, "join", raft_JOIN_test)
	runStep(t, mc, "remove", raft_REMOVE_test)
}

func raft_SNAPSHOT_test(mc *mockCluster) error {
	for i := 0; i < 1000; i++ {
		if err := mc.DoBatch([][]interface{}{
			{"SET", fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i)}, {"OK"},
		}); err != nil {
			return err
		}
	}
	if err := mc.DoBatch([][]interface{}{
		{"RAFTSNAPSHOT"}, {"OK"},
	}); err != nil {
		return err
	}
	return nil
}
func raftWaitForNumPeers(mc *mockCluster, count int) error {
	for {
		var numPeers int
		var derr error
		err := mc.DoBatch([][]interface{}{
			{"RAFTSTATS"}, {func(v interface{}) (r, e interface{}) {
				parts := strings.Split(fmt.Sprintf("%v\n", v), " ")
				for i := 0; i < len(parts); i += 2 {
					if parts[i] == "num_peers" {
						n, err := strconv.ParseInt(parts[i+1], 10, 64)
						if err != nil {
							derr = err
							return "", ""
						}
						numPeers = int(n)
					}
				}
				return "", ""
			}},
		})
		if err != nil {
			return err
		}
		if derr != nil {
			return derr
		}
		if numPeers == count {
			return nil
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func raft_JOIN_test(mc *mockCluster) error {
	if err := raftWaitForNumPeers(mc, 2); err != nil {
		return err
	}
	for i := 0; i < 9; i++ {
		if err := raft_SNAPSHOT_test(mc); err != nil {
			return err
		}
	}
	s, err := mockOpenServer(nil)
	if err != nil {
		return err
	}
	err = mc.DoBatch([][]interface{}{
		{"RAFTADDPEER", fmt.Sprintf(":%d", s.port)}, {"OK"},
	})
	if err != nil {
		return err
	}
	mc.ss = append(mc.ss, s)
	mc.ResetConn()
	if err := raftWaitForNumPeers(mc, 3); err != nil {
		return err
	}
	return nil
}

func raft_REMOVE_test(mc *mockCluster) error {
	if err := raftWaitForNumPeers(mc, 3); err != nil {
		return err
	}
	rand.Seed(time.Now().UnixNano())
	idx := rand.Int() % len(mc.ss)
	s := mc.ss[idx]
	err := mc.DoBatch([][]interface{}{
		{"RAFTREMOVEPEER", fmt.Sprintf(":%d", s.port)}, {"OK"},
	})
	if err != nil {
		return err
	}
	s.Close()
	mc.ss = append(mc.ss[:idx], mc.ss[idx+1:]...)
	mc.ResetConn()
	if err := raftWaitForNumPeers(mc, 2); err != nil {
		return err
	}
	return nil

}
