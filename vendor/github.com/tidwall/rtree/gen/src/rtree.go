// +build ignore

package rtree

import "math"

type Iterator func(item Item) bool
type Item interface {
	Rect(ctx interface{}) (min []float64, max []float64)
}

type RTree struct {
	ctx interface{}
	// BEGIN
	trTNUMDIMS *dTNUMDIMSRTree
	// END
}

func New(ctx interface{}) *RTree {
	return &RTree{
		ctx: ctx,
		// BEGIN
		trTNUMDIMS: dTNUMDIMSNew(),
		// END
	}
}

func (tr *RTree) Insert(item Item) {
	if item == nil {
		panic("nil item being added to RTree")
	}
	min, max := item.Rect(tr.ctx)
	if len(min) != len(max) {
		return // just return
		panic("invalid item rectangle")
	}
	switch len(min) {
	default:
		return // just return
		panic("invalid dimension")
		// BEGIN
	case TNUMDIMS:
		var amin, amax [TNUMDIMS]float64
		for i := 0; i < len(min); i++ {
			amin[i], amax[i] = min[i], max[i]
		}
		tr.trTNUMDIMS.Insert(amin, amax, item)
		// END
	}
}

func (tr *RTree) Remove(item Item) {
	if item == nil {
		panic("nil item being added to RTree")
	}
	min, max := item.Rect(tr.ctx)
	if len(min) != len(max) {
		return // just return
		panic("invalid item rectangle")
	}
	switch len(min) {
	default:
		return // just return
		panic("invalid dimension")
		// BEGIN
	case TNUMDIMS:
		var amin, amax [TNUMDIMS]float64
		for i := 0; i < len(min); i++ {
			amin[i], amax[i] = min[i], max[i]
		}
		tr.trTNUMDIMS.Remove(amin, amax, item)
		// END
	}
}
func (tr *RTree) Reset() {
	// BEGIN
	tr.trTNUMDIMS = dTNUMDIMSNew()
	// END
}
func (tr *RTree) Count() int {
	count := 0
	// BEGIN
	count += tr.trTNUMDIMS.Count()
	// END
	return count
}
func (tr *RTree) Search(bounds Item, iter Iterator) {
	if bounds == nil {
		panic("nil bounds being used for search")
	}
	min, max := bounds.Rect(tr.ctx)
	if len(min) != len(max) {
		return // just return
		panic("invalid item rectangle")
	}
	switch len(min) {
	default:
		return // just return
		panic("invalid dimension")
		// BEGIN
	case TNUMDIMS:
		// END
	}
	// BEGIN
	if !tr.searchTNUMDIMS(min, max, iter) {
		return
	}
	// END
}

// BEGIN
func (tr *RTree) searchTNUMDIMS(min, max []float64, iter Iterator) bool {
	var amin, amax [TNUMDIMS]float64
	for i := 0; i < TNUMDIMS; i++ {
		if i < len(min) {
			amin[i] = min[i]
			amax[i] = max[i]
		} else {
			amin[i] = math.Inf(-1)
			amax[i] = math.Inf(+1)
		}
	}
	ended := false
	tr.trTNUMDIMS.Search(amin, amax, func(dataID interface{}) bool {
		if !iter(dataID.(Item)) {
			ended = true
			return false
		}
		return true
	})
	return !ended
}

// END
