package machine

import (
	"strconv"
	"strings"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

type rectSearchArgs struct {
	kind       string
	index      string
	value      string
	matchon    bool
	match      string
	limiton    bool
	limit      int
	skipon     bool
	skip       int
	withvalues bool
	within     bool
}

func parseRectSearchArgs(bargs [][]byte) (
	rargs rectSearchArgs, err error,
) {
	// convert bargs from [][]byte to []string
	args := make([]string, len(bargs))
	for i, arg := range bargs {
		args[i] = string(arg)
	}
	switch strings.ToLower(args[0]) {
	default:
		err = errSyntaxError
		return
	case "within":
		rargs.within = true
		fallthrough
	case "intersects":
		if len(args) < 3 {
			err = finn.ErrWrongNumberOfArguments
			return
		}
		rargs.kind = args[0]
		rargs.index = args[1]
		rargs.value = args[2]
		args = args[3:]
	}
	for len(args) > 0 {
		switch strings.ToLower(args[0]) {
		default:
			err = errSyntaxError
			return
		case "match":
			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}
			rargs.match = args[0]
			rargs.matchon = true
		case "limit":
			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}
			var n uint64
			n, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			rargs.limit = int(n)
			rargs.limiton = true
		case "skip":
			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}
			var n uint64
			n, err = strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return
			}
			rargs.skip = int(n)
			rargs.skipon = true
		}
		args = args[1:]
	}
	return
}

// doRectSearch searches for intersecting rectangles on spatial indexes.
func (m *Machine) doRectSearch(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// INTERSECTS index bounds [MATCH pattern] [LIMIT limit] [SKIP skip]
	rargs, err := parseRectSearchArgs(cmd.Args)
	if err != nil {
		return nil, err
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		var results []string
		var skipcount int
		limit := rargs.limit * 2
		err := tx.Intersects(rargs.index, rargs.value,
			func(key, val string) bool {
				if isMercMetaKey(key) {
					return true
				}
				if rargs.limiton && len(results) >= limit {
					return false
				}
				if rargs.matchon && !match.Match(key, rargs.match) {
					return true
				}
				// within here
				if rargs.skipon && skipcount < rargs.skip {
					skipcount++
					return true
				}
				results = append(results, key, val)
				return true
			},
		)
		if err != nil {
			return err
		}
		conn.WriteArray(len(results))
		for _, result := range results {
			conn.WriteBulkString(result)
		}
		return nil
	})
}
