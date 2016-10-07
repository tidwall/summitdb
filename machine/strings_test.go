package machine

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func subTestStrings(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "BITCOUNT test", strings_BITCOUNT_test)
	runStep(t, mc, "BITOP test", strings_BITOP_test)
	runStep(t, mc, "GETRANGE test", strings_GETRANGE_test)
	runStep(t, mc, "SETRANGE test", strings_SETRANGE_test)
	runStep(t, mc, "SET single item", strings_SET_singleItem)
	runStep(t, mc, "GET single item", strings_GET_singleItem)
	runStep(t, mc, "STRLEN single item", strings_STRLEN_singleItem)
	runStep(t, mc, "DEL single item", strings_DEL_singleItem)
	runStep(t, mc, "APPEND test", strings_APPEND_test)
	runStep(t, mc, "INCR test", strings_INCR_test)
	runStep(t, mc, "DECR test", strings_DECR_test)
	runStep(t, mc, "INCRBY test", strings_INCRBY_test)
	runStep(t, mc, "DECRBY test", strings_DECRBY_test)
	runStep(t, mc, "INCRBYFLOAT test", strings_INCRBYFLOAT_test)
	runStep(t, mc, "GETSET test", strings_GETSET_test)
	runStep(t, mc, "MSETNX test", strings_MSETNX_test)
	for i := 1; i <= 100000; i *= 100 {
		var pl string
		if i > 1 {
			pl = "s"
		}
		runStep(t, mc, fmt.Sprintf("MSET %d item%s", i, pl), func(mc *mockCluster) error { return strings_MSET_nItems(mc, i) })
		runStep(t, mc, fmt.Sprintf("MGET %d item%s", i, pl), func(mc *mockCluster) error { return strings_MGET_nItems(mc, i) })
		runStep(t, mc, fmt.Sprintf("DEL %d item%s", i, pl), func(mc *mockCluster) error { return strings_DEL_nItems(mc, i) })
	}
	runStep(t, mc, "SETNX not exists", strings_SETNX_notExists)
	runStep(t, mc, "SETNX exists", strings_SETNX_exists)
	runStep(t, mc, "SET nx not exists", strings_SET_nxNotExists)
	runStep(t, mc, "SET nx exists", strings_SET_nxExists)
	runStep(t, mc, "SET xx not exists", strings_SET_xxNotExists)
	runStep(t, mc, "SET xx exists", strings_SET_xxExists)
	runStep(t, mc, "SET expires seconds", strings_SET_expiresSeconds)
	runStep(t, mc, "SET expires milliseconds", strings_SET_expiresMilliseconds)
	runStep(t, mc, "SETEX expires seconds", strings_SETEX_expiresSeconds)
	runStep(t, mc, "PSETEX expires milliseconds", strings_PSETEX_expiresMilliseconds)
}

func strings_BITCOUNT_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "foobar"}, {"OK"},
		{"BITCOUNT", "mykey"}, {26},
		{"BITCOUNT", "mykey", 0, 0}, {4},
		{"BITCOUNT", "mykey", 1, 1}, {6},
		{"DEL", "mykey"}, {1},
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
		{"DEL", "key1"}, {1},
	})
}
func strings_SETRANGE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", "Hello World"}, {"OK"},
		{"SETRANGE", "key1", 6, "Merck"}, {11},
		{"GET", "key1"}, {"Hello Merck"},
		{"DEL", "key1"}, {1},
	})
}

func strings_GETRANGE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "This is a string"}, {"OK"},
		{"GETRANGE", "mykey", 0, 3}, {"This"},
		{"GETRANGE", "mykey", -3, -1}, {"ing"},
		{"GETRANGE", "mykey", 0, -1}, {"This is a string"},
		{"GETRANGE", "mykey", 10, 1000}, {"string"},
		{"DEL", "mykey"}, {1},
	})
}

func strings_GETSET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"GETSET", "mykey", "val1"}, {nil},
		{"GETSET", "mykey", "val2"}, {"val1"},
		{"GET", "mykey"}, {"val2"},
		{"DEL", "mykey"}, {1},
	})
}

func strings_APPEND_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"APPEND", "mykey", "val1"}, {4},
		{"APPEND", "mykey", "val2"}, {8},
		{"GET", "mykey"}, {"val1val2"},
		{"DEL", "mykey"}, {1},
	})
}

func strings_INCR_test(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("INCR", "__key__:incr"))
	if err != nil {
		return err
	} else if resp != 1 {
		return fmt.Errorf("expected '%v', got '%v'", 1, resp)
	}
	for i := 0; i < 10; i++ {
		resp, err = redis.Int(mc.Do("INCR", "__key__:incr"))
		if err != nil {
			return err
		} else if resp != i+2 {
			return fmt.Errorf("expected '%v', got '%v'", i+2, resp)
		}
	}
	s, err := redis.String(mc.Do("GET", "__key__:incr"))
	if err != nil {
		return err
	}
	if s != "11" {
		return fmt.Errorf("expecting '%v', got '%v'", "11", s)
	}
	return nil
}

func strings_DECR_test(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("DECR", "__key__:decr"))
	if err != nil {
		return err
	} else if resp != -1 {
		return fmt.Errorf("expected '%v', got '%v'", -1, resp)
	}
	for i := 0; i < 10; i++ {
		resp, err = redis.Int(mc.Do("DECR", "__key__:decr"))
		if err != nil {
			return err
		} else if resp != 0-i-2 {
			return fmt.Errorf("expected '%v', got '%v'", 0-i-2, resp)
		}
	}
	s, err := redis.String(mc.Do("GET", "__key__:decr"))
	if err != nil {
		return err
	}
	if s != "-11" {
		return fmt.Errorf("expecting '%v', got '%v'", "-11", s)
	}
	return nil
}

func strings_INCRBY_test(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("INCRBY", "__key__:incrby", 3))
	if err != nil {
		return err
	} else if resp != 3 {
		return fmt.Errorf("expected '%v', got '%v'", 3, resp)
	}
	for i := 0; i < 10; i++ {
		resp, err = redis.Int(mc.Do("INCRBY", "__key__:incrby", 3))
		if err != nil {
			return err
		} else if resp != (i+1)*3+3 {
			return fmt.Errorf("expected '%v', got '%v'", (i+1)*3+3, resp)
		}
	}
	s, err := redis.String(mc.Do("GET", "__key__:incrby"))
	if err != nil {
		return err
	}
	if s != "33" {
		return fmt.Errorf("expecting '%v', got '%v'", "33", s)
	}
	return nil
}

func strings_DECRBY_test(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("DECRBY", "__key__:decrby", 3))
	if err != nil {
		return err
	} else if resp != -3 {
		return fmt.Errorf("expected '%v', got '%v'", -3, resp)
	}
	for i := 0; i < 10; i++ {
		resp, err = redis.Int(mc.Do("DECRBY", "__key__:decrby", 3))
		if err != nil {
			return err
		} else if resp != -((i+1)*3 + 3) {
			return fmt.Errorf("expected '%v', got '%v'", -((i+1)*3 + 3), resp)
		}
	}
	s, err := redis.String(mc.Do("GET", "__key__:decrby"))
	if err != nil {
		return err
	}
	if s != "-33" {
		return fmt.Errorf("expecting '%v', got '%v'", "-33", s)
	}
	return nil
}

func round(v float64, decimals int) float64 {
	var pow float64 = 1
	for i := 0; i < decimals; i++ {
		pow *= 10
	}
	return float64(int((v*pow)+0.5)) / pow
}

func strings_INCRBYFLOAT_test(mc *mockCluster) error {
	resp, err := redis.Float64(mc.Do("INCRBYFLOAT", "__key__:incrbyfloat", .3))
	if err != nil {
		return err
	} else if resp != .3 {
		return fmt.Errorf("expected '%v', got '%v'", .3, resp)
	}
	t := .3
	for i := 0; i < 10; i++ {
		t += .3
		resp, err := redis.Float64(mc.Do("INCRBYFLOAT", "__key__:incrbyfloat", .3))
		if err != nil {
			return err
		}
		rf := round(resp, 1)
		ef := round(t, 1)
		if rf != ef {
			return fmt.Errorf("expected '%v', got '%v'", ef, rf)
		}
	}
	resp, err = redis.Float64(mc.Do("GET", "__key__:incrbyfloat"))
	if err != nil {
		return err
	}
	rf := round(resp, 1)
	if rf != 3.3 {
		return fmt.Errorf("expecting '%v', got '%v'", 3.3, rf)
	}
	return nil
}
func strings_SET_expiresSeconds(mc *mockCluster) error {
	if err := mc.DoExpect("OK", "SET", "__key__:ex", "value", "EX", 1); err != nil {
		return err
	}
	time.Sleep(time.Second / 2)
	if err := mc.DoExpect("value", "GET", "__key__:ex"); err != nil {
		return err
	}
	time.Sleep(time.Second / 2)
	if err := mc.DoExpect(nil, "GET", "__key__:ex"); err != nil {
		return err
	}
	return nil
}

func strings_SETEX_expiresSeconds(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SETEX", "__key__:ex", 1, "value"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	time.Sleep(time.Second / 2)
	resp, err = redis.String(mc.Do("GET", "__key__:ex"))
	if err != nil {
		return err
	} else if resp != "value" {
		return fmt.Errorf("expected '%v', got '%v'", "value", resp)
	}
	time.Sleep(time.Second / 2)
	resp2, err := mc.Do("GET", "__key__:ex")
	if err != nil {
		return err
	} else if resp2 != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resp2)
	}
	return nil
}
func strings_SET_expiresMilliseconds(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SET", "__key__:ex", "value", "PX", 500))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	time.Sleep(time.Second / 4)
	resp, err = redis.String(mc.Do("GET", "__key__:ex"))
	if err != nil {
		return err
	} else if resp != "value" {
		return fmt.Errorf("expected '%v', got '%v'", "value", resp)
	}
	time.Sleep(time.Second / 4)
	resp2, err := mc.Do("GET", "__key__:ex")
	if err != nil {
		return err
	} else if resp2 != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resp2)
	}
	return nil
}

func strings_PSETEX_expiresMilliseconds(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("PSETEX", "__key__:ex", 500, "value"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	time.Sleep(time.Second / 4)
	resp, err = redis.String(mc.Do("GET", "__key__:ex"))
	if err != nil {
		return err
	} else if resp != "value" {
		return fmt.Errorf("expected '%v', got '%v'", "value", resp)
	}
	time.Sleep(time.Second / 4)
	resp2, err := mc.Do("GET", "__key__:ex")
	if err != nil {
		return err
	} else if resp2 != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resp2)
	}
	return nil
}
func strings_SET_singleItem(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SET", "__key__:single_item", "value"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	return nil
}
func strings_SETNX_notExists(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SETNX", "__key__:nx2", "value"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	return nil
}

func strings_SETNX_exists(mc *mockCluster) error {
	resp, err := mc.Do("SETNX", "__key__:nx2", "value")
	if err != nil {
		return err
	} else if resp != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resp)
	}
	return nil
}

func strings_SET_nxNotExists(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SET", "__key__:nx", "value", "NX"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	return nil
}

func strings_SET_nxExists(mc *mockCluster) error {
	resp, err := mc.Do("SET", "__key__:nx", "value", "NX")
	if err != nil {
		return err
	} else if resp != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resp)
	}
	return nil
}
func strings_SET_xxNotExists(mc *mockCluster) error {
	resp, err := mc.Do("SET", "__key__:xx", "value", "XX")
	if err != nil {
		return err
	} else if resp != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resp)
	}
	str, err := redis.String(mc.Do("SET", "__key__:xx", "value"))
	if err != nil {
		return err
	} else if str != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	return nil
}

func strings_SET_xxExists(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SET", "__key__:xx", "value", "XX"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	return nil
}
func strings_GET_singleItem(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("GET", "__key__:single_item"))
	if err != nil {
		return err
	} else if resp != "value" {
		return fmt.Errorf("expected '%v', got '%v'", "value", resp)
	}
	return nil
}
func strings_STRLEN_singleItem(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("STRLEN", "__key__:single_item"))
	if err != nil {
		return err
	} else if resp != 5 {
		return fmt.Errorf("expected '%v', got '%v'", 5, resp)
	}
	return nil
}

func strings_DEL_singleItem(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("DEL", "__key__:single_item"))
	if err != nil {
		return err
	} else if resp != 1 {
		return fmt.Errorf("expected '%v', got '%v'", 1, resp)
	}
	data, err := mc.Do("GET", "__key__:single_item")
	if err != nil {
		return err
	} else if data != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, data)
	}
	return nil
}

func strings_MSET_nItems(mc *mockCluster, n int) error {
	var args []interface{}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("__key__:%d_items:%d", n, i), fmt.Sprintf("__val__:%d_items:%d", n, i))
	}
	resp, err := redis.String(mc.Do("MSET", args...))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
	}
	return nil
}

func strings_MSETNX_test(mc *mockCluster) error {
	resp, err := redis.Int(mc.Do("MSETNX", "__key__:msetnx:key1", "Hello", "__key__:msetnx:key2", "there"))
	if err != nil {
		return err
	} else if resp != 1 {
		return fmt.Errorf("expected '%v', got '%v'", 1, resp)
	}
	resp, err = redis.Int(mc.Do("MSETNX", "__key__:msetnx:key2", "there", "__key__:msetnx:key3", "world"))
	if err != nil {
		return err
	} else if resp != 0 {
		return fmt.Errorf("expected '%v', got '%v'", 0, resp)
	}
	resps, err := redis.Values(mc.Do("MGET", "__key__:msetnx:key1", "__key__:msetnx:key2", "__key__:msetnx:key3"))
	if err != nil {
		return err
	}
	if string(resps[0].([]byte)) != "Hello" {
		return fmt.Errorf("expected '%v', got '%v'", "Hello", string(resps[0].([]byte)))
	}
	if string(resps[1].([]byte)) != "there" {
		return fmt.Errorf("expected '%v', got '%v'", "there", string(resps[1].([]byte)))
	}
	if resps[2] != nil {
		return fmt.Errorf("expected '%v', got '%v'", nil, resps[2])
	}
	return nil
}

func strings_DEL_nItems(mc *mockCluster, n int) error {
	var args []interface{}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("__key__:%d_items:%d", n, i))
	}
	resp, err := redis.Int(mc.Do("DEL", args...))
	if err != nil {
		return err
	} else if resp != n {
		return fmt.Errorf("expected '%v', got '%v'", n, resp)
	}
	resps, err := redis.Values(mc.Do("MGET", args...))
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if resps[i] != nil {
			return fmt.Errorf("expected '%v', got '%v'", nil, resps[i])
		}
	}
	return nil
}

func strings_MGET_nItems(mc *mockCluster, n int) error {
	var args []interface{}
	for i := 0; i < n; i++ {
		args = append(args, fmt.Sprintf("__key__:%d_items:%d", n, i))
	}
	resps, err := redis.Strings(mc.Do("MGET", args...))
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		val := fmt.Sprintf("__val__:%d_items:%d", n, i)
		if resps[i] != val {
			return fmt.Errorf("expected '%v', got '%v'", val, resps[i])
		}
	}
	return nil
}
