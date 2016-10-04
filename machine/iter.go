package machine

import (
	"strconv"
	"strings"

	"github.com/tidwall/buntdb"
	"github.com/tidwall/finn"
	"github.com/tidwall/less"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

type iterArgs struct {
	kind       string
	index      string
	pattern    string
	desc       bool
	limiton    bool
	limit      int
	pivoton    bool
	pivot      string
	withvalues bool
	rangeon    bool
	rangeminc  byte
	rangemin   string
	rangemaxc  byte
	rangemax   string
	matchon    bool
	match      string
}

func parseIterArgs(bargs [][]byte) (
	rargs iterArgs, err error,
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
	case "keys":
		if len(args) < 2 {
			err = finn.ErrWrongNumberOfArguments
			return
		}
		rargs.kind = "keys"
		rargs.pattern = args[1]
		args = args[2:]
	case "iter", "iterate":
		if len(args) < 2 {
			err = finn.ErrWrongNumberOfArguments
			return
		}
		rargs.kind = "iter"
		rargs.index = args[1]
		args = args[2:]
	}
	for len(args) > 0 {
		switch strings.ToLower(args[0]) {
		default:
			err = errSyntaxError
			return
		case "withvalues":
			if rargs.kind != "keys" {
				err = errSyntaxError
				return
			}
			rargs.withvalues = true
		case "range":
			if rargs.kind != "iter" {
				err = errSyntaxError
				return
			}
			rargs.rangeon = true
			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}
			if len(args[0]) != 0 && (args[0][0] == '[' || args[0][0] == '(') {
				rargs.rangeminc = args[0][0]
				rargs.rangemin = args[0][1:]
			} else if args[0] == "-inf" || args[0] == "+inf" {
				rargs.rangeminc = args[0][0]
			} else {
				rargs.rangeminc = '['
				rargs.rangemin = args[0]
			}

			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}

			if len(args[0]) != 0 && (args[0][len(args[0])-1] == ']' || args[0][len(args[0])-1] == ')') {
				rargs.rangemaxc = args[0][len(args[0])-1]
				rargs.rangemax = args[0][:len(args[0])-1]
			} else if args[0] == "-inf" || args[0] == "+inf" {
				rargs.rangemaxc = args[0][0]
			} else {
				rargs.rangemaxc = ']'
				rargs.rangemax = args[0]
			}

		case "desc":
			rargs.desc = true
		case "asc":
			rargs.desc = false
		case "pivot":
			args = args[1:]
			if len(args) == 0 {
				err = finn.ErrWrongNumberOfArguments
				return
			}
			rargs.pivot = args[0]
			rargs.pivoton = true
		case "match":
			if rargs.kind != "iter" {
				err = errSyntaxError
				return
			}
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
		}
		args = args[1:]
	}
	return
}

func (m *Machine) iterateIndex(rargs *iterArgs, conn redcon.Conn, tx *buntdb.Tx) (results []string, err error) {
	// ITER index [PIVOT value] [RANGE min max] [LIMIT limit] [DESC|ASC]
	//var min, max string
	var l less.Less
	var pivoton bool
	var pivot string
	var pastpivot bool // flag that indicates that we iterated past the pivot
	var pastrange bool
	var pivoteq bool // flag indicating that pivot compares should exclude equal-to
	iterfn := func(key, val string) bool {
		if isMercMetaKey(key) {
			return true
		}
		if rargs.limiton && len(results) >= rargs.limit*2 {
			return false
		}

		// check pivot
		if pivoton {
			if !pastpivot {
				if !rargs.desc {
					if pivoteq {
						if l.LessThanOrEqualTo(val, pivot) {
							return true
						}
					} else {
						if l.LessThan(val, pivot) {
							return true
						}
					}
				} else {
					if pivoteq {
						if rargs.desc && l.GreaterThanOrEqualTo(val, pivot) {
							return true
						}
					} else {
						if rargs.desc && l.GreaterThan(val, pivot) {
							return true
						}
					}
				}
				pastpivot = true
			}
		}

		// check pivot
		// check range
		if rargs.rangeon {
			if !pastrange {
				if !rargs.desc {
					if rargs.rangeminc == '-' {
						// accept infinity
					} else if rargs.rangeminc == '(' {
						if l.LessThanOrEqualTo(val, rargs.rangemin) {
							return true
						}
					} else {
						if l.LessThan(val, rargs.rangemin) {
							return true
						}
					}
				} else {
					if rargs.rangemaxc == '+' {
						// accept infinity
					} else if rargs.rangemaxc == ')' {
						if l.GreaterThanOrEqualTo(val, rargs.rangemax) {
							return true
						}
					} else {
						if l.GreaterThan(val, rargs.rangemax) {
							return true
						}
					}
				}
				pastrange = true
			}
			if !rargs.desc {
				if rargs.rangemaxc == '+' {
					// accept infinity
				} else if rargs.rangemaxc == ')' {
					if l.GreaterThanOrEqualTo(val, rargs.rangemax) {
						return false
					}
				} else {
					if l.GreaterThan(val, rargs.rangemax) {
						return false
					}
				}
			} else {
				if rargs.rangeminc == '-' {
					// accept infinity
				} else if rargs.rangeminc == '(' {
					if l.LessThanOrEqualTo(val, rargs.rangemin) {
						return false
					}
				} else {
					if l.LessThan(val, rargs.rangemin) {
						return false
					}
				}
			}
		}

		// check match
		if rargs.matchon && !match.Match(key, rargs.match) {
			return true
		}

		results = append(results, key, val)
		return true
	}

	err = func() error {
		// get the less function for the request index.
		// if a less function is not found then we just return.
		lessfn, err := tx.GetLess(rargs.index)
		if err != nil {
			if err == buntdb.ErrNotFound || lessfn == nil {
				return nil
			}
			return err
		}

		// get a less interface doing various logical operations
		l = less.Less(lessfn)

		// determine the pivot. it's possible that there's a PIVOT and
		// a RANGE specified. If so we should make sure that the pivot
		// is corrected to the more rational value.
		pivot = rargs.pivot
		pivoton = rargs.pivoton
		pivoteq = pivoton
		if rargs.rangeon {
			if rargs.desc {
				if rargs.rangemaxc != '+' {
					if pivoton {
						if l.LessThan(rargs.rangemax, pivot) {
							pivot = rargs.rangemax
							pivoteq = false
						}
					} else {
						pivoton = true
						pivot = rargs.rangemax
					}
				}
			} else {
				if rargs.rangeminc != '-' {
					if pivoton {
						if l.GreaterThan(rargs.rangemin, pivot) {
							pivot = rargs.rangemin
							pivoteq = false
						}
					} else {
						pivoton = true
						pivot = rargs.rangemin
					}
				}
			}
		}

		// perform the iteration
		if rargs.desc {
			if pivoton {
				return tx.DescendLessOrEqual(rargs.index, pivot, iterfn)
			}
			return tx.Descend(rargs.index, iterfn)
		}
		if pivoton {
			return tx.AscendGreaterOrEqual(rargs.index, pivot, iterfn)
		}
		return tx.Ascend(rargs.index, iterfn)
	}()
	return
}

func (m *Machine) iterateKeys(rargs *iterArgs, conn redcon.Conn, tx *buntdb.Tx) (results []string, err error) {
	// KEYS pattern [PIVOT value] [WITHVALUES] [LIMIT limit] [DESC|ASC]
	if rargs.pattern == "" {
		return nil, nil
	}
	var forever bool
	var min, max string
	if rargs.pattern[0] != '*' {
		min, max = match.Allowable(rargs.pattern)
		if rargs.pivoton {
			if rargs.desc {
				max = rargs.pivot
				if min > max {
					return nil, nil // nothing to return
				}
			} else {
				min = rargs.pivot
				if max < min {
					return nil, nil // nothing to return
				}
			}
		}
	} else {
		forever = true
		if !rargs.pivoton && !rargs.desc {
			rargs.pivoton = true
			rargs.pivot = ""
		} else {
			if rargs.desc {
				max = rargs.pivot
			} else {
				min = rargs.pivot
			}
		}
	}
	if rargs.withvalues {
		rargs.limit *= 2
	}
	err = func() error {
		if rargs.desc {
			descIter := func(key, val string) bool {
				if isMercMetaKey(key) {
					return true
				}
				if rargs.limiton && len(results) == rargs.limit {
					return false
				}
				if !forever && key < min {
					return false
				}
				if forever && rargs.pivoton && key >= rargs.pivot {
					return true // just skip this one
				}
				if match.Match(key, rargs.pattern) {
					results = append(results, key)
					if rargs.withvalues {
						results = append(results, val)
					}
				}
				return true
			}
			if max == "" && forever {
				return tx.Descend("", descIter)
			}
			return tx.DescendLessOrEqual("", max, descIter)
		}
		return tx.AscendGreaterOrEqual("", min, func(key, val string) bool {
			if isMercMetaKey(key) {
				return true
			}
			if rargs.limiton && len(results) == rargs.limit {
				return false
			}
			if !forever && key > max {
				return false
			}
			if forever && rargs.pivoton && key <= rargs.pivot {
				return true // just skip this one
			}
			if match.Match(key, rargs.pattern) {
				results = append(results, key)
				if rargs.withvalues {
					results = append(results, val)
				}
			}
			return true
		})
	}()
	return
}

// doIter executes the KEYS or ITER commands.
func (m *Machine) doIter(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	rargs, err := parseIterArgs(cmd.Args)
	if err != nil {
		return nil, err
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		var results []string
		var err error
		if rargs.kind == "iter" {
			results, err = m.iterateIndex(&rargs, conn, tx)
		} else if rargs.kind == "keys" {
			results, err = m.iterateKeys(&rargs, conn, tx)
		}
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}
		conn.WriteArray(len(results))
		for _, result := range results {
			conn.WriteBulkString(result)
		}
		return nil
	})
}
