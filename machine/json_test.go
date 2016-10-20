package machine

import "testing"

func subTestJSON(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "JSET", json_JSET_test)
	runStep(t, mc, "JGET", json_JGET_test)
	runStep(t, mc, "JDEL", json_JDEL_test)
}

func json_JSET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"JSET", "user:101", "name", "Tom"}, {"OK"},
		{"JSET", "user:101", "age", 46}, {"OK"},
		{"GET", "user:101"}, {`{"age":46,"name":"Tom"}`},
	})
}
func json_JGET_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"JSET", "user:101", "name", "Tom"}, {"OK"},
		{"JSET", "user:101", "age", 46}, {"OK"},
		{"GET", "user:101"}, {`{"age":46,"name":"Tom"}`},
		{"JGET", "user:101", "age"}, {"46"},
		{"JSET", "user:101", "a.b.c", "hello"}, {"OK"},
		{"GET", "user:101"}, {`{"a":{"b":{"c":"hello"}},"age":46,"name":"Tom"}`},
	})
}
func json_JDEL_test(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"JSET", "user:101", "name", "Tom"}, {"OK"},
		{"JSET", "user:101", "age", 46}, {"OK"},
		{"GET", "user:101"}, {`{"age":46,"name":"Tom"}`},
		{"JGET", "user:101", "age"}, {"46"},
		{"JSET", "user:101", "a.b.c", "hello"}, {"OK"},
		{"GET", "user:101"}, {`{"a":{"b":{"c":"hello"}},"age":46,"name":"Tom"}`},
		{"JDEL", "user:101", "a.b"}, {1},
		{"GET", "user:101"}, {`{"a":{},"age":46,"name":"Tom"}`},
	})
}
