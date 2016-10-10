package finn

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tidwall/raft-redcon"
	"github.com/tidwall/redcon"
)

type KVM struct {
	mu   sync.RWMutex
	keys map[string][]byte
}

func NewKVM() *KVM {
	return &KVM{
		keys: make(map[string][]byte),
	}
}
func (kvm *KVM) Command(m Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return nil, ErrUnknownCommand
	case "set":
		if len(cmd.Args) != 3 {
			return nil, ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				kvm.mu.Lock()
				defer kvm.mu.Unlock()
				kvm.keys[string(cmd.Args[1])] = cmd.Args[2]
				return nil, nil
			},
			func(v interface{}) (interface{}, error) {
				conn.WriteString("OK")
				return nil, nil
			},
		)
	case "get":
		if len(cmd.Args) != 2 {
			return nil, ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			nil,
			func(interface{}) (interface{}, error) {
				kvm.mu.RLock()
				defer kvm.mu.RUnlock()
				if val, ok := kvm.keys[string(cmd.Args[1])]; !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
				return nil, nil
			},
		)
	}
}

func (kvm *KVM) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return err
	}
	var keys map[string][]byte
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	kvm.keys = keys
	return nil
}

func (kvm *KVM) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	data, err := json.Marshal(kvm.keys)
	if err != nil {
		return err
	}
	if _, err := wr.Write(data); err != nil {
		return err
	}
	return nil
}

var killed = make(map[int]bool)
var killCond = sync.NewCond(&sync.Mutex{})

func killNodes(basePort int) {
	killCond.L.Lock()
	killed[basePort] = true
	killCond.Broadcast()
	killCond.L.Unlock()
}

func startTestNode(t testing.TB, basePort int, num int, opts *Options) {
	node := fmt.Sprintf("%d", num)

	if err := os.MkdirAll("data/"+node, 0700); err != nil {
		t.Fatal(err)
	}
	join := ""
	if node == "" {
		node = "0"
	}
	addr := fmt.Sprintf(":%d", basePort/10*10+num)
	if node != "0" {
		join = fmt.Sprintf(":%d", basePort)
	}
	n, err := Open("data/"+node, addr, join, NewKVM(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for {
		killCond.L.Lock()
		if killed[basePort] {
			killCond.L.Unlock()
			return
		}
		killCond.Wait()
		killCond.L.Unlock()
	}
}

func waitFor(t testing.TB, basePort, node int) {
	target := fmt.Sprintf(":%d", basePort/10*10+node)
	start := time.Now()
	for {
		if time.Now().Sub(start) > time.Second*10 {
			t.Fatal("timeout looking for leader")
		}
		time.Sleep(time.Second / 4)
		resp, _, err := raftredcon.Do(target, nil, []byte("raftleader"))
		if err != nil {
			continue
		}
		if len(resp) != 0 {
			return
		}
	}
}

func testDo(t testing.TB, basePort, node int, expect string, args ...string) string {
	var bargs [][]byte
	for _, arg := range args {
		bargs = append(bargs, []byte(arg))
	}
	target := fmt.Sprintf(":%d", basePort/10*10+node)
	resp, _, err := raftredcon.Do(target, nil, bargs...)
	if err != nil {
		if err.Error() == expect {
			return ""
		}
		t.Fatalf("node %d: %v", node, err)
	}
	if expect != "???" && string(resp) != expect {
		t.Fatalf("node %d: expected '%v', got '%v'", node, expect, string(resp))
	}
	return string(resp)
}

func TestVarious(t *testing.T) {
	t.Run("Level", SubTestLevel)
	t.Run("Backend", SubTestBackend)
}

func SubTestLevel(t *testing.T) {
	var level Level
	level = Level(-99)
	if level.String() != "unknown" {
		t.Fatalf("expecting '%v', got '%v'", "unknown", level.String())
	}
	level = Low
	if level.String() != "low" {
		t.Fatalf("expecting '%v', got '%v'", "low", level.String())
	}
	level = Medium
	if level.String() != "medium" {
		t.Fatalf("expecting '%v', got '%v'", "medium", level.String())
	}
	level = High
	if level.String() != "high" {
		t.Fatalf("expecting '%v', got '%v'", "high", level.String())
	}
}

func SubTestBackend(t *testing.T) {
	var backend Backend
	backend = Backend(-99)
	if backend.String() != "unknown" {
		t.Fatalf("expecting '%v', got '%v'", "unknown", backend.String())
	}
	backend = FastLog
	if backend.String() != "fastlog" {
		t.Fatalf("expecting '%v', got '%v'", "fastlog", backend.String())
	}
	backend = Bolt
	if backend.String() != "bolt" {
		t.Fatalf("expecting '%v', got '%v'", "bolt", backend.String())
	}
	backend = InMem
	if backend.String() != "inmem" {
		t.Fatalf("expecting '%v', got '%v'", "inmem", backend.String())
	}
}

func TestCluster(t *testing.T) {

	var optsArr []Options
	for _, backend := range []Backend{Bolt, FastLog, InMem} {
		for _, consistency := range []Level{Low, Medium, High} {
			optsArr = append(optsArr, Options{
				Backend:     backend,
				Consistency: consistency,
			})
		}
	}
	for i := 0; i < len(optsArr); i++ {
		func() {
			opts := optsArr[i]
			if os.Getenv("LOG") != "1" {
				opts.LogOutput = ioutil.Discard
			}
			basePort := (7480/10 + i) * 10
			tag := fmt.Sprintf("%v-%v-%d", opts.Backend, opts.Consistency, basePort)
			t.Logf("%s", tag)
			t.Run(tag, func(t *testing.T) {
				os.RemoveAll("data")
				defer os.RemoveAll("data")
				defer killNodes(basePort)
				for i := 0; i < 3; i++ {
					go startTestNode(t, basePort, i, &opts)
					waitFor(t, basePort, i)
				}
				t.Run("Leader", func(t *testing.T) { SubTestLeader(t, basePort, &opts) })
				t.Run("Set", func(t *testing.T) { SubTestSet(t, basePort, &opts) })
				t.Run("Get", func(t *testing.T) { SubTestGet(t, basePort, &opts) })
				t.Run("Snapshot", func(t *testing.T) { SubTestSnapshot(t, basePort, &opts) })
				t.Run("Ping", func(t *testing.T) { SubTestPing(t, basePort, &opts) })
				t.Run("RaftShrinkLog", func(t *testing.T) { SubTestRaftShrinkLog(t, basePort, &opts) })
				t.Run("RaftStats", func(t *testing.T) { SubTestRaftStats(t, basePort, &opts) })
				t.Run("RaftState", func(t *testing.T) { SubTestRaftState(t, basePort, &opts) })
				t.Run("AddPeer", func(t *testing.T) { SubTestAddPeer(t, basePort, &opts) })
				t.Run("RemovePeer", func(t *testing.T) { SubTestRemovePeer(t, basePort, &opts) })
			})
		}()
	}
}

func SubTestLeader(t *testing.T, basePort int, opts *Options) {
	baseAddr := fmt.Sprintf(":%d", basePort)
	testDo(t, basePort, 0, baseAddr, "raftleader")
	testDo(t, basePort, 1, baseAddr, "raftleader")
	testDo(t, basePort, 2, baseAddr, "raftleader")
}

func SubTestSet(t *testing.T, basePort int, opts *Options) {
	baseAddr := fmt.Sprintf(":%d", basePort)
	testDo(t, basePort, 0, "OK", "set", "hello", "world")
	testDo(t, basePort, 1, "TRY "+baseAddr, "set", "hello", "world")
	testDo(t, basePort, 2, "TRY "+baseAddr, "set", "hello", "world")
}

func SubTestGet(t *testing.T, basePort int, opts *Options) {
	baseAddr := fmt.Sprintf(":%d", basePort)
	testDo(t, basePort, 0, "world", "get", "hello")
	testDo(t, basePort, 1, "TRY "+baseAddr, "set", "hello", "world")
	testDo(t, basePort, 2, "TRY "+baseAddr, "set", "hello", "world")
}

func SubTestPing(t *testing.T, basePort int, opts *Options) {
	for i := 0; i < 3; i++ {
		testDo(t, basePort, i, "PONG", "ping")
		testDo(t, basePort, i, "HELLO", "ping", "HELLO")
		testDo(t, basePort, i, "ERR wrong number of arguments for 'ping' command", "ping", "HELLO", "WORLD")
	}
}

func SubTestRaftShrinkLog(t *testing.T, basePort int, opts *Options) {
	for i := 0; i < 3; i++ {
		if opts.Backend == Bolt {
			testDo(t, basePort, i, "ERR log is not shrinkable", "raftshrinklog")
		} else {
			testDo(t, basePort, i, "OK", "raftshrinklog")
		}
		testDo(t, basePort, i, "ERR wrong number of arguments for 'raftshrinklog' command", "raftshrinklog", "abc")
	}
}
func SubTestRaftStats(t *testing.T, basePort int, opts *Options) {
	for i := 0; i < 3; i++ {
		resp := testDo(t, basePort, i, "???", "raftstats")
		if !strings.Contains(resp, "applied_index") || !strings.Contains(resp, "num_peers") {
			t.Fatal("expected values")
		}
		testDo(t, basePort, i, "ERR wrong number of arguments for 'raftstats' command", "raftstats", "abc")
	}
}
func SubTestRaftState(t *testing.T, basePort int, opts *Options) {
	for i := 0; i < 3; i++ {
		if i == 0 {
			testDo(t, basePort, i, "Leader", "raftstate")
		} else {
			testDo(t, basePort, i, "Follower", "raftstate")
		}
		testDo(t, basePort, i, "ERR wrong number of arguments for 'raftstate' command", "raftstate", "abc")
	}
}
func SubTestSnapshot(t *testing.T, basePort int, opts *Options) {
	// insert 1000 items
	for i := 0; i < 1000; i++ {
		testDo(t, basePort, 0, "OK", "set", fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i))
	}
	testDo(t, basePort, 0, "OK", "raftsnapshot")
	testDo(t, basePort, 1, "OK", "raftsnapshot")
	testDo(t, basePort, 2, "OK", "raftsnapshot")
}
func SubTestAddPeer(t *testing.T, basePort int, opts *Options) {
	baseAddr := fmt.Sprintf(":%d", basePort)
	go startTestNode(t, basePort, 3, opts)
	waitFor(t, basePort, 3)
	testDo(t, basePort, 3, baseAddr, "raftleader")
	testDo(t, basePort, 3, "TRY "+baseAddr, "set", "hello", "world")
	testDo(t, basePort, 3, "OK", "raftsnapshot")
}

func SubTestRemovePeer(t *testing.T, basePort int, opts *Options) {
	baseAddr := fmt.Sprintf(":%d", basePort)
	testDo(t, basePort, 1, "TRY "+baseAddr, "raftremovepeer", fmt.Sprintf(":%d3", basePort/10))
	testDo(t, basePort, 0, "OK", "raftremovepeer", fmt.Sprintf(":%d3", basePort/10))
	testDo(t, basePort, 0, "peer is unknown", "raftremovepeer", fmt.Sprintf(":%d3", basePort/10))
}

func BenchmarkCluster(t *testing.B) {
	os.RemoveAll("data")
	defer os.RemoveAll("data")
	for i := 0; i < 3; i++ {
		go startTestNode(t, 7480, i, &Options{LogOutput: ioutil.Discard})
		waitFor(t, 7480, i)
	}
	t.Run("PL", func(t *testing.B) {
		pl := []int{1, 4, 16, 64}
		for i := 0; i < len(pl); i++ {
			func(pl int) {
				t.Run(fmt.Sprintf("%d", pl), func(t *testing.B) {
					t.Run("Ping", func(t *testing.B) { SubBenchmarkPing(t, pl) })
					t.Run("Set", func(t *testing.B) { SubBenchmarkSet(t, pl) })
					t.Run("Get", func(t *testing.B) { SubBenchmarkGet(t, pl) })
				})
			}(pl[i])
		}
	})
}
func testDial(t testing.TB, node int) (net.Conn, *bufio.ReadWriter) {
	conn, err := net.Dial("tcp", fmt.Sprintf(":748%d", node))
	if err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}
func buildCommand(args ...string) []byte {
	var buf []byte
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

func testConnDo(t testing.TB, rw *bufio.ReadWriter, pl int, expect string, cmd []byte) {
	for i := 0; i < pl; i++ {
		rw.Write(cmd)
	}
	if err := rw.Flush(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(expect))
	for i := 0; i < pl; i++ {
		if _, err := io.ReadFull(rw, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != expect {
			t.Fatalf("expected '%v', got '%v'", expect, string(buf))
		}
	}
}

func SubBenchmarkPing(t *testing.B, pipeline int) {
	conn, rw := testDial(t, 0)
	defer conn.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i += pipeline {
		n := pipeline
		if t.N-i < pipeline {
			n = t.N - i
		}
		testConnDo(t, rw, n, "+PONG\r\n", []byte("*1\r\n$4\r\nPING\r\n"))
	}
}

func SubBenchmarkSet(t *testing.B, pipeline int) {
	conn, rw := testDial(t, 0)
	defer conn.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i += pipeline {
		n := pipeline
		if t.N-i < pipeline {
			n = t.N - i
		}
		testConnDo(t, rw, n, "+OK\r\n", buildCommand("set", fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i)))
	}
}

func SubBenchmarkGet(t *testing.B, pipeline int) {
	conn, rw := testDial(t, 0)
	defer conn.Close()
	t.ResetTimer()
	for i := 0; i < t.N; i += pipeline {
		n := pipeline
		if t.N-i < pipeline {
			n = t.N - i
		}
		testConnDo(t, rw, n, "$-1\r\n", buildCommand("get", "key:na"))
	}
}
