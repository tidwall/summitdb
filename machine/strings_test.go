package machine

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

func subTestStrings(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "SET single item", strings_SET_singleItem)
	runStep(t, mc, "GET single item", strings_GET_singleItem)
	runStep(t, mc, "STRLEN single item", strings_STRLEN_singleItem)
	runStep(t, mc, "DEL single item", strings_DEL_singleItem)
	for i := 1; i <= 100000; i *= 100 {
		var pl string
		if i > 1 {
			pl = "s"
		}
		runStep(t, mc, fmt.Sprintf("MSET %d item%s", i, pl), func(mc *mockCluster) error { return strings_MSET_nItems(mc, i) })
		runStep(t, mc, fmt.Sprintf("MGET %d item%s", i, pl), func(mc *mockCluster) error { return strings_MGET_nItems(mc, i) })
		runStep(t, mc, fmt.Sprintf("DEL %d item%s", i, pl), func(mc *mockCluster) error { return strings_DEL_nItems(mc, i) })
	}
	runStep(t, mc, "SET nx not exists", strings_SET_nxNotExists)
	runStep(t, mc, "SET nx exists", strings_SET_nxExists)
	runStep(t, mc, "SET xx not exists", strings_SET_xxNotExists)
	runStep(t, mc, "SET xx exists", strings_SET_xxExists)
	runStep(t, mc, "SET expires seconds", strings_SET_expiresSeconds)
	runStep(t, mc, "SET expires milliseconds", strings_SET_expiresMilliseconds)
}

func strings_SET_expiresSeconds(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SET", "__key__:ex", "value", "EX", 1))
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

func strings_SET_singleItem(mc *mockCluster) error {
	resp, err := redis.String(mc.Do("SET", "__key__:single_item", "value"))
	if err != nil {
		return err
	} else if resp != "OK" {
		return fmt.Errorf("expected '%v', got '%v'", "OK", resp)
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
