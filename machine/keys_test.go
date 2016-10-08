package machine

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func subTestKeys(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "KEYS", keys_KEYS_iterate)
	runStep(t, mc, "TYPE", keys_TYPE_test)
	runStep(t, mc, "EXISTS", keys_EXISTS_test)
	runStep(t, mc, "RESTORE", keys_RESTORE_test)
	runStep(t, mc, "DUMP", keys_DUMP_test)
	runStep(t, mc, "PERSIST", keys_PERSIST_test)
	runStep(t, mc, "EXPIRE", keys_EXPIRE_test)
	runStep(t, mc, "EXPIREAT", keys_EXPIREAT_test)
	runStep(t, mc, "PEXPIRE", keys_PEXPIRE_test)
	runStep(t, mc, "PEXPIREAT", keys_PEXPIREAT_test)
	runStep(t, mc, "TTL", keys_TTL_test)
	runStep(t, mc, "PTTL", keys_PTTL_test)
	runStep(t, mc, "RENAME", keys_RENAME_test)
	runStep(t, mc, "RENAMENX", keys_RENAMENX_test)
	runStep(t, mc, "DBSIZE", keys_DBSIZE_test)
	runStep(t, mc, "FLUSHDB", keys_FLUSHDB_test)
	runStep(t, mc, "TIME", keys_TIME_test)
	runStep(t, mc, "DEL", keys_DEL_test)
	runStep(t, mc, "PDEL", keys_PDEL_test)
	runStep(t, mc, "MASSINSERT", keys_MASSINSERT_test)
}
func keys_TYPE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "value"}, {"OK"},
		{"TYPE", "key1"}, {"string"},
		{"TYPE", "key2"}, {"none"},
	})
}
func keys_DUMP_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "value"}, {"OK"},
		{"DUMP", "key1"}, {"value"},
		{"DUMP", "key2"}, {nil},
	})
}
func keys_RESTORE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "value"}, {"OK"},
		{"RESTORE", "key2", 0, "value2"}, {"OK"},
		{"GET", "key2"}, {"value2"},
		{"RESTORE", "key2", 0, "value2"}, {"BUSYKEY Target key name already exists."},
		{"RESTORE", "key2", 0, "value2", "REPLACE"}, {"OK"},
		{"RESTORE", "key3", 250, "value3"}, {"OK"},
		{"GET", "key3"}, {"value3"},
		{time.Millisecond * 500}, {},
		{"GET", "key3"}, {nil},
	})
}
func keys_EXISTS_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "value"}, {"OK"},
		{"SET", "key2", "value"}, {"OK"},
		{"EXISTS", "key1"}, {1},
		{"EXISTS", "key1", "key2"}, {2},
		{"EXISTS", "key2", "key3"}, {1},
		{"EXISTS", "key3", "key4"}, {0},
	})
}
func keys_PERSIST_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"EXPIRE", "mykey", 2}, {1},
		{"PERSIST", "mykey"}, {1},
		{"PERSIST", "mykey"}, {0},
	})
}
func keys_TTL_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"EXPIRE", "mykey", 2}, {1},
		{time.Second / 4}, {}, // sleep
		{"TTL", "mykey"}, {1},
	})
}
func keys_PTTL_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"EXPIRE", "mykey", 2}, {1},
		{time.Second / 4}, {}, // sleep
		{"PTTL", "mykey"}, {func(v interface{}) (r, e interface{}) {
			n, _ := strconv.ParseInt(fmt.Sprintf("%v", v), 10, 64)
			if n > 0 && n < 2000 {
				return "", ""
			}
			return v, "1000"
		}},
	})
}
func keys_EXPIRE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"EXPIRE", "mykey", 1}, {1},
		{time.Second / 4}, {}, // sleep
		{"GET", "mykey"}, {"value"},
		{time.Second}, {}, // sleep
		{"GET", "mykey"}, {nil},
	})
}
func keys_EXPIREAT_test(mc *mockCluster) error {
	unix := int(time.Now().Add(time.Second * 2).Unix())
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"EXPIREAT", "mykey", unix}, {1},
		{time.Second}, {}, // sleep
		{"GET", "mykey"}, {"value"},
		{time.Second}, {}, // sleep
		{"GET", "mykey"}, {nil},
	})
}
func keys_PEXPIRE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"PEXPIRE", "mykey", 500}, {1},
		{time.Second / 8}, {}, // sleep
		{"GET", "mykey"}, {"value"},
		{time.Second / 2}, {}, // sleep
		{"GET", "mykey"}, {nil},
	})
}
func keys_PEXPIREAT_test(mc *mockCluster) error {
	unix := int(time.Duration(time.Now().Add(time.Second*2).UnixNano()) / time.Millisecond)
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"PEXPIREAT", "mykey", unix}, {1},
		{time.Second}, {}, // sleep
		{"GET", "mykey"}, {"value"},
		{time.Second}, {}, // sleep
		{"GET", "mykey"}, {nil},
	})
}
func keys_KEYS_iterate(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "5"}, {"OK"},
		{"SET", "key2", "3"}, {"OK"},
		{"SET", "key3", "7"}, {"OK"},
		{"SET", "key1", "2"}, {"OK"},
		{"SET", "key5", "9"}, {"OK"},
		{"SET", "key4", "1"}, {"OK"},
		{"SET", "key7", "2"}, {"OK"},
		{"SET", "key8", "12"}, {"OK"},

		{"KEYS", "*"}, {"[key1 key2 key3 key4 key5 key6 key7 key8]"},
		{"KEYS", "a*"}, {"[]"},
		{"KEYS", "*", "PIVOT", "key1", "LIMIT", 6}, {"[key2 key3 key4 key5 key6 key7]"},
		{"KEYS", "*", "PIVOT", "key8", "LIMIT", 6, "DESC"}, {"[key7 key6 key5 key4 key3 key2]"},
		{"KEYS", "*2", "PIVOT", "key1"}, {"[key2]"},
		{"KEYS", "k*", "PIVOT", "key1"}, {"[key2 key3 key4 key5 key6 key7 key8]"},
		{"KEYS", "k*", "PIVOT", "key8", "DESC"}, {"[key7 key6 key5 key4 key3 key2 key1]"},

		{"KEYS"}, {"ERR wrong number of arguments for 'KEYS' command"},
	})
}

func keys_MASSINSERT_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"MASSINSERT", 100000}, {100000},
		{"DBSIZE"}, {100000},
	})
}

func keys_RENAME_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "5"}, {"OK"},
		{"RENAME", "key6", "key7"}, {"OK"},
		{"GET", "key7"}, {"5"},
		{"GET", "key6"}, {nil},
	})
}

func keys_RENAMENX_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"FLUSHDB"}, {"OK"},

		{"SET", "key6", "5"}, {"OK"},
		{"RENAMENX", "key6", "key7"}, {1},
		{"GET", "key7"}, {"5"},
		{"SET", "key8", "6"}, {"OK"},
		{"RENAMENX", "key7", "key8"}, {0},
		{"GET", "key8"}, {"6"},
		{"FLUSHDB"}, {"OK"},
	})
}

func keys_DBSIZE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"FLUSHDB"}, {"OK"},

		{"SET", "key1", "5"}, {"OK"},
		{"SET", "key2", "5"}, {"OK"},
		{"SET", "key3", "5"}, {"OK"},
		{"SET", "key4", "5"}, {"OK"},
		{"SET", "key5", "5"}, {"OK"},
		{"DBSIZE"}, {5},
		{"FLUSHDB"}, {"OK"},
	})
}

func keys_FLUSHDB_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"FLUSHDB"}, {"OK"},

		{"SET", "key1", "5"}, {"OK"},
		{"SET", "key2", "5"}, {"OK"},
		{"SET", "key3", "5"}, {"OK"},
		{"SET", "key4", "5"}, {"OK"},
		{"SET", "key5", "5"}, {"OK"},

		{"DBSIZE"}, {5},
		{"FLUSHDB"}, {"OK"},
		{"DBSIZE"}, {0},
	})
}

func keys_TIME_test(mc *mockCluster) error {
	resp, err := redis.Values(mc.Do("TIME"))
	if err != nil {
		return err
	}
	if b, ok := resp[0].([]uint8); ok {
		n, _ := strconv.ParseInt(string(b), 10, 64)
		if n == 0 {
			return fmt.Errorf("expecting > 0, got 0")
		}
		return nil
	}
	return fmt.Errorf("invalid format: %v", resp)
}
func keys_DEL_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "hello"}, {"OK"},
		{"GET", "mykey"}, {"hello"},
		{"DEL", "mykey"}, {1},
		{"GET", "mykey"}, {nil},
		{"DEL", "mykey"}, {0},
		{"MSET", "1", "1", "2", "2", "3", "3"}, {"OK"},
		{"DEL", "1", "2", "3", "4"}, {3},
	})
}

func keys_PDEL_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key:0:1", "hello"}, {"OK"},
		{"SET", "key:0:2", "hello"}, {"OK"},
		{"SET", "key:0:3", "hello"}, {"OK"},
		{"SET", "key:1:1", "hello"}, {"OK"},
		{"SET", "key:2:2", "hello"}, {"OK"},
		{"SET", "key:3:3", "hello"}, {"OK"},
		{"DBSIZE"}, {6},
		{"PDEL", "key:*:3"}, {2},
		{"DBSIZE"}, {4},
		{"KEYS", "*"}, {"[key:0:1 key:0:2 key:1:1 key:2:2]"},
		{"PDEL", "key:0:*"}, {2},
		{"DBSIZE"}, {2},
		{"KEYS", "*"}, {"[key:1:1 key:2:2]"},
	})
}
