// Package less provide a simple utility for relational comparisons on a less
// function.
package less

import "sort"

func lessThan(a, b string, less func(a, b string) bool) bool {
	return less(a, b)
}
func lessThanOrEqualTo(a, b string, less func(a, b string) bool) bool {
	return less(a, b) || !less(b, a)
}
func greaterThan(a, b string, less func(a, b string) bool) bool {
	return less(b, a)
}
func greaterThanOrEqualTo(a, b string, less func(a, b string) bool) bool {
	return less(b, a) || !less(a, b)
}
func equalTo(a, b string, less func(a, b string) bool) bool {
	return !less(a, b) && !less(b, a)
}

// Less represents a less function that uses string as the parameter type.
type Less func(a, b string) bool

// LessThan returns true for "a < b"
func (less Less) LessThan(a, b string) bool {
	return lessThan(a, b, less)
}

// LessThanOrEqualTo returns true for "a <= b"
func (less Less) LessThanOrEqualTo(a, b string) bool {
	return lessThanOrEqualTo(a, b, less)
}

// GreaterThan returns true for "a > b"
func (less Less) GreaterThan(a, b string) bool {
	return greaterThan(a, b, less)
}

// GreaterThanOrEqualTo returns true for "a >= b"
func (less Less) GreaterThanOrEqualTo(a, b string) bool {
	return greaterThanOrEqualTo(a, b, less)
}

// EqualTo returns true for "a == b"
func (less Less) EqualTo(a, b string) bool {
	return equalTo(a, b, less)
}

type stringSlice struct {
	less func(a, b string) bool
	arr  []string
}

func (a stringSlice) Len() int {
	return len(a.arr)
}
func (a stringSlice) Less(i, j int) bool {
	return a.less(a.arr[i], a.arr[j])
}
func (a stringSlice) Swap(i, j int) {
	a.arr[i], a.arr[j] = a.arr[j], a.arr[i]
}

// Sort sorts data. The sort is not guaranteed to be stable.
func (less Less) Sort(arr []string) {
	sort.Sort(stringSlice{less, arr})
}

// Stable sorts data while keeping the original order of equal elements.
func (less Less) Stable(arr []string) {
	sort.Stable(stringSlice{less, arr})
}

func bytesLessThan(a, b []byte, less func(a, b []byte) bool) bool {
	return less(a, b)
}
func bytesLessThanOrEqualTo(a, b []byte, less func(a, b []byte) bool) bool {
	return less(a, b) || !less(b, a)
}
func bytesGreaterThan(a, b []byte, less func(a, b []byte) bool) bool {
	return less(b, a)
}
func bytesGreaterThanOrEqualTo(a, b []byte, less func(a, b []byte) bool) bool {
	return less(b, a) || !less(a, b)
}
func bytesEqualTo(a, b []byte, less func(a, b []byte) bool) bool {
	return !less(a, b) && !less(b, a)
}

// BytesLess represents a less function that uses []byte as the parameter type.
type BytesLess func(a, b []byte) bool

// LessThan returns true for "a < b"
func (less BytesLess) LessThan(a, b []byte) bool {
	return bytesLessThan(a, b, less)
}

// LessThanOrEqualTo returns true for "a <= b"
func (less BytesLess) LessThanOrEqualTo(a, b []byte) bool {
	return bytesLessThanOrEqualTo(a, b, less)
}

// GreaterThan returns true for "a > b"
func (less BytesLess) GreaterThan(a, b []byte) bool {
	return bytesGreaterThan(a, b, less)
}

// GreaterThanOrEqualTo returns true for "a >= b"
func (less BytesLess) GreaterThanOrEqualTo(a, b []byte) bool {
	return bytesGreaterThanOrEqualTo(a, b, less)
}

// EqualTo returns true for "a == b"
func (less BytesLess) EqualTo(a, b []byte) bool {
	return bytesEqualTo(a, b, less)
}

type bytesSlice struct {
	less func(a, b []byte) bool
	arr  [][]byte
}

func (a bytesSlice) Len() int {
	return len(a.arr)
}
func (a bytesSlice) Less(i, j int) bool {
	return a.less(a.arr[i], a.arr[j])
}
func (a bytesSlice) Swap(i, j int) {
	a.arr[i], a.arr[j] = a.arr[j], a.arr[i]
}

// Sort sorts data. The sort is not guaranteed to be stable.
func (less BytesLess) Sort(arr [][]byte) {
	sort.Sort(bytesSlice{less, arr})
}

// Stable sorts data while keeping the original order of equal elements.
func (less BytesLess) Stable(arr [][]byte) {
	sort.Stable(bytesSlice{less, arr})
}
