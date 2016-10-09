package machine

import "testing"

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
		{"EVAL", `merc.call("set", "1", "2");return merc.call("get", "1")`, 0}, {"2"},
		{"GET", "1"}, {"2"},
	})
}
func scripts_READONLY_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"EVALRO", `merc.call("set", "1", "2");return merc.call("get", "1")`, 0}, {"tx not writable"},
		{"SET", "1", "2"}, {"OK"},
		{"EVALRO", `return merc.call("get", "1")`, 0}, {"2"},
	})
}
func scripts_EVALSHA_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"EVALSHA", `asdf`, 0}, {"NOSCRIPT No matching script. Please use EVAL."},
		{"SCRIPT", "LOAD", `return merc.call("get", "1")`}, {"4dc9f4b3bfe6718dbc2306f340dc27af670d7f91"},
		{"SCRIPT", "LOAD", `return merc.call("set", "1", ARGV[0])`}, {"4d141d818890364b99a7390750cc3e011a6b96e7"},
		{"EVALSHA", "4d141d818890364b99a7390750cc3e011a6b96e7", 0, "value"}, {"OK"},
		{"EVALSHARO", "4dc9f4b3bfe6718dbc2306f340dc27af670d7f91", 0}, {"value"},
		{"FLUSHDB"}, {"OK"},
		{"EVALSHARO", "4dc9f4b3bfe6718dbc2306f340dc27af670d7f91", 0}, {nil},
		{"SCRIPT", "LOAD", `return merc.call("get", "1")`}, {"4dc9f4b3bfe6718dbc2306f340dc27af670d7f91"},
		{"SCRIPT", "LOAD", `return merc.call("set", "1", ARGV[0])`}, {"4d141d818890364b99a7390750cc3e011a6b96e7"},
		{"EVALSHA", "4d141d818890364b99a7390750cc3e011a6b96e7", 0, "value"}, {"OK"},
		{"EVALSHARO", "4dc9f4b3bfe6718dbc2306f340dc27af670d7f91", 0}, {"value"},
		{"SCRIPT", "FLUSH"}, {"OK"},
		{"EVALSHARO", "4dc9f4b3bfe6718dbc2306f340dc27af670d7f91", 0}, {"NOSCRIPT No matching script. Please use EVAL."},
	})
}
