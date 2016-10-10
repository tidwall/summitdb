package rtree

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type tRect []float64

func (r *tRect) Arr() []float64 {
	return []float64(*r)
}
func (r *tRect) Rect(ctx interface{}) (min, max []float64) {
	return r.Arr()[:len(r.Arr())/2], r.Arr()[len(r.Arr())/2:]
}

func (r *tRect) String() string {
	min, max := r.Rect(nil)
	return fmt.Sprintf("%v,%v", min, max)
}
func tRandRect(dims int) *tRect {
	if dims == -1 {
		dims = rand.Int()%4 + 1
	}
	r := tRect(make([]float64, dims*2))
	for j := 0; j < dims; j++ {
		minf := rand.Float64()*200 - 100
		maxf := rand.Float64()*200 - 100
		if minf > maxf {
			minf, maxf = maxf, minf
		}
		r[j] = minf
		r[dims+j] = maxf
	}
	return &r
}

type tPoint struct {
	x, y float64
}

func (r *tPoint) Rect(ctx interface{}) (min, max []float64) {
	return []float64{r.x, r.y}, []float64{r.x, r.y}
}
func tRandPoint() *tPoint {
	return &tPoint{
		rand.Float64()*200 - 100,
		rand.Float64()*200 - 100,
	}
}
func TestRTree(t *testing.T) {
	tr := New("hello")
	zeroPoint := &tRect{0, 0, 0, 0}
	tr.Insert(&tRect{10, 10, 10, 10, 20, 20, 20, 20})
	tr.Insert(&tRect{10, 10, 10, 20, 20, 20})
	tr.Insert(&tRect{10, 10, 20, 20})
	tr.Insert(&tRect{10, 20})
	tr.Insert(zeroPoint)
	if tr.Count() != 5 {
		t.Fatalf("expecting %v, got %v", 5, tr.Count())
	}

	var count int
	tr.Search(&tRect{0, 0, 0, 100, 100, 5}, func(item Item) bool {
		count++
		return true
	})

	if count != 3 {
		t.Fatalf("expecting %v, got %v", 3, count)
	}
	tr.Remove(zeroPoint)
	count = 0
	tr.Search(&tRect{0, 0, 0, 100, 100, 5}, func(item Item) bool {
		count++
		return true
	})
	if count != 2 {
		t.Fatalf("expecting %v, got %v", 2, count)
	}
}

func TestInsertDelete(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	n := 50000
	tr := New(nil)
	var r2arr []*tRect
	for i := 0; i < n; i++ {
		r := tRandRect(-1)
		if len(r.Arr()) == 4 {
			r2arr = append(r2arr, r)
		}
		tr.Insert(r)
	}
	if tr.Count() != n {
		t.Fatalf("expecting %v, got %v", n, tr.Count())
	}
	var count int
	tr.Search(&tRect{-100, -100, -100, -100, 100, 100, 100, 100}, func(item Item) bool {
		if len(item.(*tRect).Arr()) == 4 {
			count++
		}
		return true
	})
	p := float64(count) / float64(n)

	if p < .23 || p > .27 {
		t.Fatalf("bad random range, expected between 0.24-0.26, got %v", p)
	}
	for _, i := range rand.Perm(len(r2arr)) {
		tr.Remove(r2arr[i])
	}
	total := tr.Count() + count
	if total != n {
		t.Fatalf("expected %v, got %v", n, total)
	}
}
func TestPoints(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	n := 25000
	tr := New(nil)
	var points []*tPoint
	for i := 0; i < n; i++ {
		r := tRandPoint()
		points = append(points, r)
		tr.Insert(r)
	}
	if tr.Count() != n {
		t.Fatalf("expecting %v, got %v", n, tr.Count())
	}
	var count int
	tr.Search(&tRect{-100, -100, -100, -100, 100, 100, 100, 100}, func(item Item) bool {
		count++
		return true
	})

	if count != n {
		t.Fatalf("expecting %v, got %v", n, count)
	}
	for _, i := range rand.Perm(len(points)) {
		tr.Remove(points[i])
	}
	total := tr.Count() + count
	if total != n {
		t.Fatalf("expected %v, got %v", n, total)
	}
}
func BenchmarkInsert(t *testing.B) {
	t.StopTimer()
	rand.Seed(time.Now().UnixNano())
	tr := New(nil)
	var points []*tPoint
	for i := 0; i < t.N; i++ {
		points = append(points, tRandPoint())
	}
	t.StartTimer()
	for i := 0; i < t.N; i++ {
		tr.Insert(points[i])
	}
	t.StopTimer()
	count := tr.Count()
	if count != t.N {
		t.Fatalf("expected %v, got %v", t.N, count)
	}

	t.StartTimer()
}
