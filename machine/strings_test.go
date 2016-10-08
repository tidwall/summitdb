package machine

import (
	"testing"
	"time"
)

func subTestStrings(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "INCRBYFLOAT", strings_INCRBYFLOAT_test)
	runStep(t, mc, "GET", strings_GET_test)
	runStep(t, mc, "SET", strings_SET_test)
	runStep(t, mc, "STRLEN", strings_STRLEN_test)
	runStep(t, mc, "BITPOS", strings_BITPOS_test)
	runStep(t, mc, "SETBIT", strings_SETBIT_test)
	runStep(t, mc, "GETBIT", strings_GETBIT_test)
	runStep(t, mc, "BITCOUNT", strings_BITCOUNT_test)
	runStep(t, mc, "BITOP", strings_BITOP_test)
	runStep(t, mc, "GETRANGE", strings_GETRANGE_test)
	runStep(t, mc, "SETRANGE", strings_SETRANGE_test)
	runStep(t, mc, "APPEND", strings_APPEND_test)
	runStep(t, mc, "INCR", strings_INCR_test)
	runStep(t, mc, "DECR", strings_DECR_test)
	runStep(t, mc, "INCRBY", strings_INCRBY_test)
	runStep(t, mc, "DECRBY", strings_DECRBY_test)
	runStep(t, mc, "GETSET", strings_GETSET_test)
	runStep(t, mc, "MSETNX", strings_MSETNX_test)
	runStep(t, mc, "MSET", strings_MSET_test)
	runStep(t, mc, "MGET", strings_MGET_test)
	runStep(t, mc, "SETNX", strings_SETNX_test)
	runStep(t, mc, "PSETEX", strings_PSETEX_test)
	runStep(t, mc, "SETEX", strings_SETEX_test)
}

func strings_BITCOUNT_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "foobar"}, {"OK"},
		{"BITCOUNT", "mykey"}, {26},
		{"BITCOUNT", "mykey", 0, 0}, {4},
		{"BITCOUNT", "mykey", 1, 1}, {6},
	})
}

func strings_BITOP_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "foobar"}, {"OK"},
		{"SET", "key2", "abcdef"}, {"OK"},
		{"BITOP", "AND", "dest", "key1", "key2"}, {6},
		{"GET", "dest"}, {"`bc`ab"},
		{"BITOP", "NOT", "dest", "key1"}, {6},
		{"GET", "dest"}, {"\x99\x90\x90\x9d\x9e\x8d"},
		{"BITOP", "or", "dest", "key1", "key2"}, {6},
		{"GET", "dest"}, {"goofev"},
		{"BITOP", "xor", "dest", "key1", "key2"}, {6},
		{"GET", "dest"}, {"\a\r\x0c\x06\x04\x14"},
	})
}
func strings_SETRANGE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "Hello World"}, {"OK"},
		{"SETRANGE", "key1", 6, "Merck"}, {11},
		{"GET", "key1"}, {"Hello Merck"},
	})
}

func strings_GETRANGE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "This is a string"}, {"OK"},
		{"GETRANGE", "mykey", 0, 3}, {"This"},
		{"GETRANGE", "mykey", -3, -1}, {"ing"},
		{"GETRANGE", "mykey", 0, -1}, {"This is a string"},
		{"GETRANGE", "mykey", 10, 1000}, {"string"},
	})
}

func strings_GETSET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"GETSET", "mykey", "val1"}, {nil},
		{"GETSET", "mykey", "val2"}, {"val1"},
		{"GET", "mykey"}, {"val2"},
	})
}

func strings_APPEND_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"APPEND", "mykey", "val1"}, {4},
		{"APPEND", "mykey", "val2"}, {8},
		{"GET", "mykey"}, {"val1val2"},
	})
}

func strings_GETBIT_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SETBIT", "mykey", 7, 1}, {0},
		{"GETBIT", "mykey", 0}, {0},
		{"GETBIT", "mykey", 7}, {1},
		{"GETBIT", "mykey", 100}, {0},
	})
}

func strings_SETBIT_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SETBIT", "mykey", 7, 1}, {0},
		{"SETBIT", "mykey", 7, 0}, {1},
		{"GET", "mykey"}, {"\u0000"},
	})
}

func strings_BITPOS_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "\xff\xf0\x00"}, {"OK"},
		{"BITPOS", "mykey", 0}, {12},
		{"SET", "mykey", "\x00\xff\xf0"}, {"OK"},
		{"BITPOS", "mykey", 1, 0}, {8},
		{"BITPOS", "mykey", 1, 2}, {16},
		{"SET", "mykey", "\x00\x00\x00"}, {"OK"},
		{"BITPOS", "mykey", 1}, {-1},
	})
}
func strings_INCR_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"INCR", "mykey"}, {1},
		{"INCR", "mykey"}, {2},
		{"INCR", "mykey"}, {3},
		{"INCR", "mykey"}, {4},
		{"GET", "mykey"}, {4},
	})
}
func strings_GET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "hello"}, {"OK"},
		{"GET", "mykey"}, {"hello"},
	})
}
func strings_SET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "value"}, {"OK"},
		{"GET", "mykey"}, {"value"},
		{"DEL", "mykey"}, {1},
		{"SET", "mykey", "value", "NX"}, {"OK"},
		{"SET", "mykey", "value", "NX"}, {nil},
		{"DEL", "mykey"}, {1},
		{"SET", "mykey", "value", "XX"}, {nil},
		{"SET", "mykey", "value"}, {"OK"},
		{"SET", "mykey", "hello", "XX"}, {"OK"},
		{"GET", "mykey"}, {"hello"},
		{"DEL", "mykey"}, {1},
		{"SET", "mykey", "value", "PX", 500}, {"OK"},
		{time.Second / 4}, {}, //sleep
		{"GET", "mykey"}, {"value"},
		{time.Second / 4}, {}, //sleep
		{"GET", "mykey"}, {nil},
		{"SET", "mykey", "value", "EX", 1}, {"OK"},
		{time.Second / 2}, {}, //sleep
		{"GET", "mykey"}, {"value"},
		{time.Second / 2}, {}, //sleep
		{"GET", "mykey"}, {nil},
	})
}

func strings_SETNX_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SETNX", "mykey", "value"}, {"OK"},
		{"SETNX", "mykey", "value"}, {nil},
	})
}

func strings_MSET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"MSET", "key1", "val1", "key2", "val2", "key3", "val3"}, {"OK"},
		{"MGET", "key1", "key2", "key3"}, {"[val1 val2 val3]"},
	})
}

func strings_MGET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"MSET", "key1", "val1", "key2", "val2", "key3", "val3"}, {"OK"},
		{"MGET", "key1", "key2", "key3"}, {"[val1 val2 val3]"},
	})
}
func strings_MSETNX_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"MSETNX", "key1", "val1", "key2", "val2", "key3", "val3"}, {1},
		{"MGET", "key1", "key2", "key3"}, {"[val1 val2 val3]"},
		{"MSETNX", "key3", "val3", "key4", "val4", "key5", "val5"}, {0},
		{"MGET", "key3", "key4", "key5"}, {"[val3 nil nil]"},
	})
}
func strings_STRLEN_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "hello"}, {"OK"},
		{"STRLEN", "mykey"}, {5},
	})
}

func strings_DECR_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"DECR", "mykey"}, {-1},
		{"DECR", "mykey"}, {-2},
		{"DECR", "mykey"}, {-3},
		{"DECR", "mykey"}, {-4},
		{"GET", "mykey"}, {-4},
	})
}

func strings_INCRBY_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"INCRBY", "mykey", 3}, {3},
		{"INCRBY", "mykey", 3}, {6},
		{"INCRBY", "mykey", 3}, {9},
		{"INCRBY", "mykey", 3}, {12},
		{"GET", "mykey"}, {12},
	})
}

func strings_DECRBY_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"DECRBY", "mykey", 3}, {-3},
		{"DECRBY", "mykey", 3}, {-6},
		{"DECRBY", "mykey", 3}, {-9},
		{"DECRBY", "mykey", 3}, {-12},
		{"GET", "mykey"}, {-12},
	})
}

func strings_INCRBYFLOAT_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"INCRBYFLOAT", "mykey", 0.3}, {exfloat(0.3, 1)},
		{"INCRBYFLOAT", "mykey", 0.3}, {exfloat(0.6, 1)},
		{"INCRBYFLOAT", "mykey", 0.3}, {exfloat(0.9, 1)},
		{"GET", "mykey"}, {exfloat(0.9, 1)},
	})
}

func strings_SETEX_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SETEX", "mykey", 1, "value"}, {"OK"},
		{time.Second / 2}, {}, // sleep
		{"GET", "mykey"}, {"value"},
		{time.Second / 2}, {}, // sleep
		{"GET", "mykey"}, {nil},
	})
}

func strings_PSETEX_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"PSETEX", "mykey", 500, "value"}, {"OK"},
		{time.Second / 4}, {}, // sleep
		{"GET", "mykey"}, {"value"},
		{time.Second / 4}, {}, // sleep
		{"GET", "mykey"}, {nil},
	})
}
