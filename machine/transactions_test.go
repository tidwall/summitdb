package machine

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
)

func subTestTransactions(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "SET", transactions_SET_test)
	runStep(t, mc, "GET", transactions_GET_test)
	runStep(t, mc, "MULTI", transactions_MULTI_test)
}

func transactions_MULTI_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"MULTI"}, {"OK"},
		{"SET", "mykey", "value1"}, {"QUEUED"},
		{"SET", "mykey", "value2"}, {"QUEUED"},
		{"EXEC"}, {"[OK OK]"},
		{"GET", "mykey"}, {"value2"},

		{"MULTI"}, {"OK"},
		{"SET", "mykey"}, {"ERR wrong number of arguments for 'SET' command"},
		{"SET", "mykey", "value2"}, {"QUEUED"},
		{"EXEC"}, {"EXECABORT Transaction discarded because of previous errors."},
		{"GET", "mykey"}, {"value2"},

		{"MULTI"}, {"OK"},
		{"SET", "mykey", "value3"}, {"QUEUED"},
		{"SET", "mykey", "value4"}, {"QUEUED"},
		{"DISCARD"}, {"OK"},
		{"GET", "mykey"}, {"value2"},

		{"MULTI"}, {"OK"},
		{"DISCARD"}, {"OK"},

		{"MULTI"}, {"OK"},
		{"EXEC"}, {"[]"},

		{"DISCARD"}, {"ERR DISCARD without MULTI"},
		{"EXEC"}, {"ERR EXEC without MULTI"},

		{"MULTI"}, {"OK"},
		{"MULTI"}, {"ERR MULTI calls can not be nested"},
		{"DISCARD"}, {"OK"},

		{"MULTI"}, {"OK"},
		{"GET", "mykey"}, {"QUEUED"},
		{"DBSIZE"}, {"QUEUED"},
		{"EXEC"}, {"[value2 1]"},
	})
}

func transactions_SET_test(mc *mockCluster) error {
	s := mc.ss[rand.Int()%len(mc.ss)]
	for {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", s.port))
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("" +
			"SET key1 foobar\r\n" +
			"SET key2 foobar\r\n" +
			"SET key3 foobar\r\n",
		)); err != nil {
			return err
		}
		buf := make([]byte, 10000)
		n, err := conn.Read(buf)
		if err != nil {
			return err
		}
		lines := strings.Split(string(buf[:n]), "\r\n")
		if len(lines) != 4 {
			return fmt.Errorf("expected '4', got '%v'", len(lines))
		}
		if lines[3] != "" {
			return fmt.Errorf("expected '', got '%v'", lines[3])
		}
		lines = lines[:3]

		if lines[0] == "+OK" {
			for i := 1; i < len(lines); i++ {
				if lines[i] != "+OK" {
					return fmt.Errorf("expected '+OK', got '%v'", lines[i])
				}
			}
			return nil
		} else if strings.HasPrefix(lines[0], "-TRY ") {
			for i := 1; i < len(lines); i++ {
				if lines[i] != lines[0] {
					return fmt.Errorf("expected '%v', got '%v'", lines[0], lines[i])
				}
			}
			n, err := strconv.ParseInt(lines[0][6:], 10, 64)
			if err != nil {
				return err
			}
			s = mc.ServerForPort(int(n))
			continue
		}
		return fmt.Errorf("expected '%v', got '%v'", "-TRY or +OK", lines[0])
	}
}

func transactions_GET_test(mc *mockCluster) error {
	s := mc.ss[rand.Int()%len(mc.ss)]
	for {
		conn, err := net.Dial("tcp", fmt.Sprintf(":%d", s.port))
		if err != nil {
			return err
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("" +
			"GET key1\r\n" +
			"GET key2\r\n" +
			"GET key3\r\n",
		)); err != nil {
			return err
		}
		buf := make([]byte, 10000)
		n, err := conn.Read(buf)
		if err != nil {
			return err
		}
		lines := strings.Split(string(buf[:n]), "\r\n")
		if len(lines) != 4 {
			return fmt.Errorf("expected '4', got '%v'", len(lines))
		}
		if lines[3] != "" {
			return fmt.Errorf("expected '', got '%v'", lines[3])
		}
		lines = lines[:3]

		if lines[0] == "$-1" {
			for i := 1; i < len(lines); i++ {
				if lines[i] != "$-1" {
					return fmt.Errorf("expected '$-1', got '%v'", lines[i])
				}
			}
			return nil
		} else if strings.HasPrefix(lines[0], "-TRY ") {
			for i := 1; i < len(lines); i++ {
				if lines[i] != lines[0] {
					return fmt.Errorf("expected '%v', got '%v'", lines[0], lines[i])
				}
			}
			n, err := strconv.ParseInt(lines[0][6:], 10, 64)
			if err != nil {
				return err
			}
			s = mc.ServerForPort(int(n))
			continue
		}
		return fmt.Errorf("expected '%v', got '%v'", "-TRY or $-1", lines[0])
	}
}
