package machine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/collate"
	"github.com/tidwall/finn"
	"github.com/tidwall/gjson"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

const indexKeyPrefix = sdbMetaPrefix + "index:"

type indexArgsIndex struct {
	Kind      string `json:"kind,omitempty"`
	Path      string `json:"path,omitempty"`
	CS        bool   `json:"cs,omitempty"`
	CollateOn bool   `json:"collate_on,omitempty"`
	Collate   string `json:"collate,omitempty"`
	Desc      bool   `json:"desc,omitempty"`
}

type indexArgs struct {
	Name        string           `json:"name,omitempty"`
	Pattern     string           `json:"pattern,omitempty"`
	SpatialOn   bool             `json:"spatial_on,omitempty"`
	SpatialPath string           `json:"spatial_path,omitempty"`
	Indexes     []indexArgsIndex `json:"indexes,omitempty"`
}

func (iargs indexArgs) Equals(rargs indexArgs) bool {
	if iargs.Name != rargs.Name ||
		iargs.Pattern != rargs.Pattern ||
		iargs.SpatialOn != rargs.SpatialOn ||
		iargs.SpatialPath != rargs.SpatialPath {
		return false
	}
	if len(iargs.Indexes) != len(rargs.Indexes) {
		return false
	}
	for i, iidx := range iargs.Indexes {
		ridx := rargs.Indexes[i]
		if iidx.Kind != ridx.Kind ||
			iidx.Path != ridx.Path ||
			iidx.CS != ridx.CS ||
			iidx.CollateOn != ridx.CollateOn ||
			iidx.Collate != ridx.Collate ||
			iidx.Desc != ridx.Desc {
			return false
		}
	}
	return true
}

func parseIndexArgs(cmd redcon.Command) (rargs indexArgs, err error) {
	args := cmd.Args
	if len(args) < 4 {
		err = finn.ErrWrongNumberOfArguments
		return
	}
	rargs.Name = string(args[1])
	rargs.Pattern = string(args[2])
	args = args[3:]
outer:
	for len(args) > 0 {
		var idx indexArgsIndex
		idx.Kind = strings.ToLower(string(args[0]))
		switch idx.Kind {
		default:
			err = errSyntaxError
			return
		case "text", "int", "float", "uint":
		case "spatial":
			if len(rargs.Indexes) > 0 {
				err = errSyntaxError
				return
			}
			rargs.SpatialOn = true
			args = args[1:]
			for len(args) > 0 {
				switch strings.ToLower(string(args[0])) {
				default:
					err = errSyntaxError
					return
				case "path":
					args = args[1:]
					if len(args) == 0 {
						err = finn.ErrWrongNumberOfArguments
						return
					}
					rargs.SpatialPath = string(args[0])
					args = args[1:]
				}
			}
			rargs.Indexes = append(rargs.Indexes, idx)
			break outer
		case "json":
			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}
			idx.Path = string(args[0])
		}
		args = args[1:]
		if idx.Kind != "spatial" {
		loop:
			for len(args) > 0 {
				switch strings.ToLower(string(args[0])) {
				default:
					break loop
				case "cs":
					if idx.Kind != "text" && idx.Kind != "json" {
						break loop
					}
					idx.CS = true
				case "collate":
					if idx.Kind != "text" && idx.Kind != "json" {
						break loop
					}
					idx.CollateOn = true
					args = args[1:]
					if len(args) == 0 {
						err = finn.ErrWrongNumberOfArguments
						return
					}
					idx.Collate = string(args[0])
				case "desc":
					idx.Desc = true
				case "asc":
					idx.Desc = false
				}
				args = args[1:]
			}
		}
		rargs.Indexes = append(rargs.Indexes, idx)
	}
	return
}

func indexRectPath(path string) func(s string) (min, max []float64) {
	return func(s string) (min, max []float64) {
		s = gjson.Get(s, path).String()
		return buntdb.IndexRect(s)
	}
}

func dbSetIndex(tx *buntdb.Tx, rargs indexArgs) error {
	// execute
	if err := tx.DropIndex(rargs.Name); err != nil && err != buntdb.ErrNotFound {
		return err
	}
	if rargs.SpatialOn {
		if rargs.SpatialPath == "" {
			err := tx.CreateSpatialIndex(rargs.Name, rargs.Pattern, buntdb.IndexRect)
			if err != nil {
				return err
			}
		} else {
			err := tx.CreateSpatialIndex(rargs.Name, rargs.Pattern, indexRectPath(rargs.SpatialPath))
			if err != nil {
				return err
			}
		}
	} else {
		var lessers []func(a, b string) bool
		for _, idx := range rargs.Indexes {
			var lesser func(a, b string) bool
			switch idx.Kind {
			default:
				return errSyntaxError
			case "text":
				if idx.CollateOn {
					lesser = collate.IndexString(idx.Collate)
				} else if idx.CS {
					lesser = buntdb.IndexBinary
				} else {
					lesser = buntdb.IndexString
				}
			case "json":
				if idx.CollateOn {
					lesser = collate.IndexJSON(idx.Collate, idx.Path)
				} else if idx.CS {
					lesser = buntdb.IndexJSONCaseSensitive(idx.Path)
				} else {
					lesser = buntdb.IndexJSON(idx.Path)
				}
			case "int":
				lesser = buntdb.IndexInt
			case "uint":
				lesser = buntdb.IndexUint
			case "float":
				lesser = buntdb.IndexFloat
			}
			if idx.Desc {
				lesser = buntdb.Desc(lesser)
			}
			lessers = append(lessers, lesser)
		}
		err := tx.CreateIndex(rargs.Name, rargs.Pattern, lessers...)
		if err != nil {
			return err
		}
	}
	data, err := json.Marshal(rargs)
	if err != nil {
		return err
	}
	if _, _, err := tx.Set(indexKeyPrefix+rargs.Name, string(data), nil); err != nil {
		return err
	}
	return err
}

func (m *Machine) doSetIndex(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SETINDEX name pattern SPATIAL [PATH path]
	// SETINDEX name pattern TEXT [CS] [COLLATE collate] [ASC|DESC]
	// SETINDEX name pattern JSON path [CS] [COLLATE collate] [ASC|DESC]
	// SETINDEX name pattern INT|FLOAT|UINT [ASC|DESC]
	rargs, err := parseIndexArgs(cmd)
	if err != nil {
		return nil, err
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if err := dbSetIndex(tx, rargs); err != nil {
			return nil, err
		}
		return nil, nil
	}, func(v interface{}) error {
		conn.WriteString("OK")
		return nil
	})
}

func (m *Machine) doDelIndex(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// DELINDEX name
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		if err := tx.DropIndex(string(cmd.Args[1])); err != nil {
			if err == buntdb.ErrNotFound {
				return 0, nil
			}
			return nil, err
		}
		if _, err := tx.Delete(indexKeyPrefix + string(cmd.Args[1])); err != nil {
			return nil, err
		}
		return 1, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doIndexes(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// INDEXES pattern [DETAILS]
	if len(cmd.Args) != 2 && len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	pattern := string(cmd.Args[1])
	var details bool
	if len(cmd.Args) == 3 {
		if strings.ToLower(string(cmd.Args[2])) != "details" {
			return nil, errSyntaxError
		}
		details = true
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		var ierr error
		indexes := make(map[string]indexArgs)
		if err := tx.AscendGreaterOrEqual("", indexKeyPrefix, func(key, val string) bool {
			if !strings.HasPrefix(key, indexKeyPrefix) {
				return false
			}
			name := key[len(indexKeyPrefix):]
			if match.Match(name, pattern) {
				var rargs indexArgs
				if details {
					if err := json.Unmarshal([]byte(val), &rargs); err != nil {
						ierr = fmt.Errorf("parsing index '%v': %v", name, err)
						return false
					}
				}
				indexes[name] = rargs
			}
			return true
		}); err != nil {
			return err
		}
		if ierr != nil {
			return ierr
		}
		names := make([]string, 0, len(indexes))
		for name := range indexes {
			if match.Match(name, pattern) {
				names = append(names, name)
			}
		}
		if details {
			conn.WriteArray(len(names) * 3)
		} else {
			conn.WriteArray(len(names))
		}
		for _, name := range names {
			if !match.Match(name, pattern) {
				continue
			}
			oidx := indexes[name]
			conn.WriteBulkString(name)
			if details {
				conn.WriteBulkString(oidx.Pattern)
				conn.WriteArray(len(oidx.Indexes))
				for _, idx := range oidx.Indexes {
					var parts []string
					parts = append(parts, idx.Kind)
					if idx.Kind == "spatial" {
						if oidx.SpatialPath != "" {
							parts = append(parts, "path", oidx.SpatialPath)
						}
					} else {
						if idx.Kind == "json" {
							parts = append(parts, idx.Path)
						}
						if idx.CollateOn {
							parts = append(parts, "collate", idx.Collate)
						}
						if idx.CS {
							parts = append(parts, "cs")
						}
						if idx.Desc {
							parts = append(parts, "desc")
						}
					}
					conn.WriteArray(len(parts))
					for _, part := range parts {
						conn.WriteBulkString(part)
					}
				}
			}
		}
		return nil
	})
}
