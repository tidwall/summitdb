package machine

import "testing"

func subTestIndexes(t *testing.T, mc *mockCluster) {
	runStep(t, mc, "text", indexes_SETINDEX_text)
	runStep(t, mc, "ints", indexes_SETINDEX_ints)
	runStep(t, mc, "binary", indexes_SETINDEX_binary)
	runStep(t, mc, "collate text", indexes_SETINDEX_collateText)
	runStep(t, mc, "collate num", indexes_SETINDEX_collateNum)
	runStep(t, mc, "json", indexes_SETINDEX_json)
	runStep(t, mc, "spatial", indexes_SETINDEX_spatial)
	runStep(t, mc, "spatial path", indexes_SETINDEX_spatialPath)
}

func indexes_SETINDEX_text(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "5"}, {"OK"},
		{"SET", "key2", "3"}, {"OK"},
		{"SET", "key3", "7"}, {"OK"},
		{"SET", "key1", "2"}, {"OK"},
		{"SET", "key5", "9"}, {"OK"},
		{"SET", "key4", "1"}, {"OK"},
		{"SET", "key7", "2"}, {"OK"},
		{"SET", "key8", "12"}, {"OK"},
		{"SETINDEX", "myindex", "*", "TEXT"}, {"OK"},
		{"ITER", "myindex"}, {"[key4 1 key8 12 key1 2 key7 2 key2 3 key6 5 key3 7 key5 9]"},
		{"SETINDEX", "myindex", "*", "TEXT", "DESC"}, {"OK"},
		{"ITER", "myindex"}, {"[key5 9 key3 7 key6 5 key2 3 key1 2 key7 2 key8 12 key4 1]"},
		{"INDEXES", "*"}, {"[myindex]"},
		{"DELINDEX", "myindex"}, {1},
		{"INDEXES", "*"}, {"[]"},
	})
}

func indexes_SETINDEX_ints(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "5"}, {"OK"},
		{"SET", "key2", "3"}, {"OK"},
		{"SET", "key3", "7"}, {"OK"},
		{"SET", "key1", "2"}, {"OK"},
		{"SET", "key5", "9"}, {"OK"},
		{"SET", "key4", "1"}, {"OK"},
		{"SET", "key7", "2"}, {"OK"},
		{"SET", "key8", "12"}, {"OK"},
		{"SETINDEX", "myindex", "*", "INT"}, {"OK"},
		{"ITER", "myindex"}, {"[key4 1 key1 2 key7 2 key2 3 key6 5 key3 7 key5 9 key8 12]"},
		{"SETINDEX", "myindex", "*", "INT", "DESC"}, {"OK"},
		{"ITER", "myindex"}, {"[key8 12 key5 9 key3 7 key6 5 key2 3 key1 2 key7 2 key4 1]"},
		{"ITER", "myindex", "RANGE", "7", "4"}, {"[key3 7 key6 5]"},
		{"ITER", "myindex", "RANGE", "(7", "4", "MATCH", "k*"}, {"[key6 5]"},
		{"ITER", "myindex", "MATCH", "*6"}, {"[key6 5]"},
	})
}

func indexes_SETINDEX_binary(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "A"}, {"OK"},
		{"SET", "key2", "b"}, {"OK"},
		{"SET", "key3", "C"}, {"OK"},
		{"SET", "key4", "d"}, {"OK"},
		{"SET", "key1", "E"}, {"OK"},
		{"SET", "key5", "f"}, {"OK"},
		{"SETINDEX", "myindex", "*", "BINARY"}, {"OK"},
		{"ITER", "myindex"}, {"[key6 A key3 C key1 E key2 b key4 d key5 f]"},
	})
}

func indexes_SETINDEX_collateText(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "ñ"}, {"OK"},
		{"SET", "key2", "Ñ"}, {"OK"},
		{"SET", "key3", "Í"}, {"OK"},
		{"SET", "key4", "Ó"}, {"OK"},
		{"SET", "key1", "Á"}, {"OK"},
		{"SET", "key5", "á"}, {"OK"},
		{"SETINDEX", "myindex", "*", "TEXT", "COLLATE", "SPANISH_CI"}, {"OK"},
		{"ITER", "myindex"}, {"[key1 Á key5 á key3 Í key2 Ñ key6 ñ key4 Ó]"},
	})
}
func indexes_SETINDEX_collateNum(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", "5"}, {"OK"},
		{"SET", "key2", "3"}, {"OK"},
		{"SET", "key3", "7"}, {"OK"},
		{"SET", "key1", "2"}, {"OK"},
		{"SET", "key5", "9"}, {"OK"},
		{"SET", "key4", "1"}, {"OK"},
		{"SET", "key7", "2"}, {"OK"},
		{"SET", "key8", "12"}, {"OK"},
		{"SETINDEX", "myindex", "*", "TEXT", "COLLATE", "EN_NUM"}, {"OK"},
		{"ITER", "myindex"}, {"[key4 1 key1 2 key7 2 key2 3 key6 5 key3 7 key5 9 key8 12]"},
	})
}
func indexes_SETINDEX_json(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key6", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`}, {"OK"},
		{"SET", "key2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`}, {"OK"},
		{"SET", "key3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`}, {"OK"},
		{"SET", "key1", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`}, {"OK"},

		{"SETINDEX", "name", "*", "JSON", "name.last"}, {"OK"},
		{"ITER", "name"}, {`[` +
			`key3 {"name":{"first":"Carol","last":"Anderson"},"age":52} ` +
			`key1 {"name":{"first":"Alan","last":"Cooper"},"age":28} ` +
			`key6 {"name":{"first":"Tom","last":"Johnson"},"age":38} ` +
			`key2 {"name":{"first":"Janet","last":"Prichard"},"age":47}` +
			`]`},

		{"SETINDEX", "age", "*", "JSON", "age"}, {"OK"},
		{"ITER", "age"}, {`[` +
			`key1 {"name":{"first":"Alan","last":"Cooper"},"age":28} ` +
			`key6 {"name":{"first":"Tom","last":"Johnson"},"age":38} ` +
			`key2 {"name":{"first":"Janet","last":"Prichard"},"age":47} ` +
			`key3 {"name":{"first":"Carol","last":"Anderson"},"age":52}` +
			`]`},

		{"SETINDEX", "age", "*", "JSON", "age", "DESC"}, {"OK"},
		{"ITER", "age"}, {`[` +
			`key3 {"name":{"first":"Carol","last":"Anderson"},"age":52} ` +
			`key2 {"name":{"first":"Janet","last":"Prichard"},"age":47} ` +
			`key6 {"name":{"first":"Tom","last":"Johnson"},"age":38} ` +
			`key1 {"name":{"first":"Alan","last":"Cooper"},"age":28}` +
			`]`},
	})
}

func indexes_SETINDEX_spatial(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", `[10 15 12]`}, {"OK"},
		{"SET", "key2", `[21 12 10]`}, {"OK"},
		{"SET", "key3", `[19 32 22]`}, {"OK"},
		{"SET", "key4", `[11 10 16]`}, {"OK"},
		{"SET", "key5", `[16 27 11]`}, {"OK"},
		{"SETINDEX", "myindex", "*", "SPATIAL"}, {"OK"},
		{"RECT", "myindex", "[12],[20]"}, {"[key3 [19 32 22] key5 [16 27 11]]"},
		{"RECT", "myindex", "[10],[20]"}, {"[key1 [10 15 12] key3 [19 32 22] key4 [11 10 16] key5 [16 27 11]]"},
		{"RECT", "myindex", "[16],[+inf]"}, {"[key2 [21 12 10] key3 [19 32 22] key5 [16 27 11]]"},
		{"RECT", "myindex", "[-inf],[+inf]"}, {"[key1 [10 15 12] key2 [21 12 10] key3 [19 32 22] key4 [11 10 16] key5 [16 27 11]]"},
		{"RECT", "myindex", "[10 -inf 13],[20 +inf 19]"}, {"[key4 [11 10 16]]"},
		{"RECT", "myindex", "[16],[+inf]", "MATCH", "key*", "SKIP", 1, "LIMIT", 1}, {"[key3 [19 32 22]]"},
	})
}

func indexes_SETINDEX_spatialPath(mc *mockCluster) error {
	return mc.DoBatch([][]interface{}{
		{"SET", "key1", `{"r":"[10 15 12]"}`}, {"OK"},
		{"SET", "key2", `{"r":"[21 12 10]"}`}, {"OK"},
		{"SET", "key3", `{"r":"[19 32 22]"}`}, {"OK"},
		{"SET", "key4", `{"r":"[11 10 16]"}`}, {"OK"},
		{"SET", "key5", `{"r":"[16 27 11]"}`}, {"OK"},
		{"SETINDEX", "myindex", "*", "SPATIAL", "PATH", "r"}, {"OK"},
		{"INDEXES", "*", "DETAILS"}, {"[myindex * [[spatial path r]]]"},
		{"RECT", "myindex", `{"r":"[12],[20]"}`}, {`[key3 {"r":"[19 32 22]"} key5 {"r":"[16 27 11]"}]`},
		{"RECT", "myindex", `{"r":"[10],[20]"}`}, {`[key1 {"r":"[10 15 12]"} key3 {"r":"[19 32 22]"} key4 {"r":"[11 10 16]"} key5 {"r":"[16 27 11]"}]`},
		{"RECT", "myindex", `{"r":"[16],[+inf]"}`}, {`[key2 {"r":"[21 12 10]"} key3 {"r":"[19 32 22]"} key5 {"r":"[16 27 11]"}]`},
		{"RECT", "myindex", `{"r":"[-inf],[+inf]"}`}, {`[key1 {"r":"[10 15 12]"} key2 {"r":"[21 12 10]"} key3 {"r":"[19 32 22]"} key4 {"r":"[11 10 16]"} key5 {"r":"[16 27 11]"}]`},
		{"RECT", "myindex", `{"r":"[10 -inf 13],[20 +inf 19]"}`}, {`[key4 {"r":"[11 10 16]"}]`},
	})
}
