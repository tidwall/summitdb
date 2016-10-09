package machine

import (
	"crypto/sha1"
	"fmt"
	"testing"
)

func subTestScripts(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "basic", scripts_SIMPLE_test)
	runStep(t, mc, "set data", scripts_SET_test)
	runStep(t, mc, "readonly", scripts_READONLY_test)
	runStep(t, mc, "sha", scripts_EVALSHA_test)
}
func scripts_SIMPLE_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"EVAL", `return 1`, 0}, {1},
		{"EVAL", `return KEYS[0]+KEYS[1]+ARGV[0]+ARGV[1]`, 2, "k1", "k2", "a1", "a2"}, {"k1k2a1a2"},
	})
}
func scripts_SET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"EVAL", `sdb.call("set", "1", "2");return sdb.call("get", "1")`, 0}, {"2"},
		{"GET", "1"}, {"2"},
	})
}
func scripts_READONLY_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"EVALRO", `sdb.call("set", "1", "2");return sdb.call("get", "1")`, 0}, {"tx not writable"},
		{"SET", "1", "2"}, {"OK"},
		{"EVALRO", `return sdb.call("get", "1")`, 0}, {"2"},
	})
}
func scripts_EVALSHA_test(mc *mockCluster) error {
	scriptSet := `return sdb.call("set", "1", ARGV[0])`
	scriptGet := `return sdb.call("get", "1")`
	shaSet := fmt.Sprintf("%x", sha1.Sum([]byte(scriptSet)))
	shaGet := fmt.Sprintf("%x", sha1.Sum([]byte(scriptGet)))
	return mc.DoBatch([][]interface{}{
		{"EVALSHA", `asdf`, 0}, {"NOSCRIPT No matching script. Please use EVAL."},
		{"SCRIPT", "LOAD", scriptSet}, {shaSet},
		{"SCRIPT", "LOAD", scriptGet}, {shaGet},
		{"EVALSHA", shaSet, 0, "value"}, {"OK"},
		{"EVALSHARO", shaGet, 0}, {"value"},
		{"FLUSHDB"}, {"OK"},
		{"EVALSHARO", shaGet, 0}, {nil},
		{"SCRIPT", "LOAD", scriptSet}, {shaSet},
		{"SCRIPT", "LOAD", scriptGet}, {shaGet},
		{"EVALSHA", shaSet, 0, "value"}, {"OK"},
		{"EVALSHARO", shaGet, 0}, {"value"},
		{"SCRIPT", "FLUSH"}, {"OK"},
		{"EVALSHARO", shaGet, 0}, {"NOSCRIPT No matching script. Please use EVAL."},
	})
}
