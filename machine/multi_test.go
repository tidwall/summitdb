package machine

import "testing"

func subTestMulti(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "MULTI", multi_MULTI_test)
}

func multi_MULTI_test(mc *mockCluster) error {
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
