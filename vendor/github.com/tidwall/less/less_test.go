package less

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/tidwall/gjson"
)

func stringLess(a, b string) bool {
	return a < b
}

func bytesLess(a, b []byte) bool {
	return bytes.Compare(a, b) == -1
}

func intLess(a, b string) bool {
	an, _ := strconv.ParseInt(a, 10, 64)
	bn, _ := strconv.ParseInt(b, 10, 64)
	return an < bn
}

func TestInts(t *testing.T) {
	l := Less(intLess)
	if !l.EqualTo("2", "2") {
		t.Fatal("fail")
	}
	if l.EqualTo("2", "12") {
		t.Fatal("fail")
	}
	if !l.LessThan("2", "12") {
		t.Fatal("fail")
	}
	if l.LessThan("12", "2") {
		t.Fatal("fail")
	}
	if !l.LessThanOrEqualTo("2", "12") {
		t.Fatal("fail")
	}
	if !l.LessThanOrEqualTo("2", "2") {
		t.Fatal("fail")
	}
	if l.LessThanOrEqualTo("12", "2") {
		t.Fatal("fail")
	}
	if !l.GreaterThan("12", "2") {
		t.Fatal("fail")
	}
	if l.GreaterThan("2", "12") {
		t.Fatal("fail")
	}
	if !l.GreaterThanOrEqualTo("12", "2") {
		t.Fatal("fail")
	}
	if !l.GreaterThanOrEqualTo("12", "12") {
		t.Fatal("fail")
	}
	if l.GreaterThanOrEqualTo("2", "12") {
		t.Fatal("fail")
	}
}
func TestString(t *testing.T) {
	l := Less(stringLess)
	if !l.EqualTo("a", "a") {
		t.Fatal("fail")
	}
	if l.EqualTo("a", "b") {
		t.Fatal("fail")
	}
	if !l.LessThan("a", "b") {
		t.Fatal("fail")
	}
	if l.LessThan("b", "a") {
		t.Fatal("fail")
	}
	if !l.LessThanOrEqualTo("a", "b") {
		t.Fatal("fail")
	}
	if !l.LessThanOrEqualTo("a", "a") {
		t.Fatal("fail")
	}
	if l.LessThanOrEqualTo("b", "a") {
		t.Fatal("fail")
	}
	if !l.GreaterThan("b", "a") {
		t.Fatal("fail")
	}
	if l.GreaterThan("a", "b") {
		t.Fatal("fail")
	}
	if !l.GreaterThanOrEqualTo("b", "a") {
		t.Fatal("fail")
	}
	if !l.GreaterThanOrEqualTo("b", "b") {
		t.Fatal("fail")
	}
	if l.GreaterThanOrEqualTo("a", "b") {
		t.Fatal("fail")
	}
}
func TestBytes(t *testing.T) {
	l := BytesLess(bytesLess)
	if !l.EqualTo([]byte("a"), []byte("a")) {
		t.Fatal("fail")
	}
	if l.EqualTo([]byte("a"), []byte("b")) {
		t.Fatal("fail")
	}
	if !l.LessThan([]byte("a"), []byte("b")) {
		t.Fatal("fail")
	}
	if l.LessThan([]byte("b"), []byte("a")) {
		t.Fatal("fail")
	}
	if !l.LessThanOrEqualTo([]byte("a"), []byte("b")) {
		t.Fatal("fail")
	}
	if !l.LessThanOrEqualTo([]byte("a"), []byte("a")) {
		t.Fatal("fail")
	}
	if l.LessThanOrEqualTo([]byte("b"), []byte("a")) {
		t.Fatal("fail")
	}
	if !l.GreaterThan([]byte("b"), []byte("a")) {
		t.Fatal("fail")
	}
	if l.GreaterThan([]byte("a"), []byte("b")) {
		t.Fatal("fail")
	}
	if !l.GreaterThanOrEqualTo([]byte("b"), []byte("a")) {
		t.Fatal("fail")
	}
	if !l.GreaterThanOrEqualTo([]byte("b"), []byte("b")) {
		t.Fatal("fail")
	}
	if l.GreaterThanOrEqualTo([]byte("a"), []byte("b")) {
		t.Fatal("fail")
	}
}

func ExampleLess_Sort() {
	intLess := func(a, b string) bool {
		an, _ := strconv.ParseInt(a, 10, 64)
		bn, _ := strconv.ParseInt(b, 10, 64)
		return an < bn
	}
	l := Less(intLess)
	items := []string{"34", "25", "9", "1", "23", "12", "22", "201", "128", "134"}
	l.Sort(items)
	for i := 0; i < len(items); i++ {
		fmt.Printf("%v\n", items[i])
	}
	// Output:
	//1
	//9
	//12
	//22
	//23
	//25
	//34
	//128
	//134
	//201
}

func ExampleBytesLess_Sort() {
	intLess := func(a, b []byte) bool {
		an, _ := strconv.ParseInt(string(a), 10, 64)
		bn, _ := strconv.ParseInt(string(b), 10, 64)
		return an < bn
	}
	l := BytesLess(intLess)
	items := [][]byte{[]byte("34"), []byte("25"), []byte("9"), []byte("1"), []byte("23"),
		[]byte("12"), []byte("22"), []byte("201"), []byte("128"), []byte("134")}
	l.Sort(items)
	for i := 0; i < len(items); i++ {
		fmt.Printf("%v\n", string(items[i]))
	}
	// Output:
	//1
	//9
	//12
	//22
	//23
	//25
	//34
	//128
	//134
	//201
}

func ExampleLessStable() {
	intLess := func(a, b string) bool {
		an, _ := strconv.ParseInt(a, 10, 64)
		bn, _ := strconv.ParseInt(b, 10, 64)
		return an < bn
	}
	l := Less(intLess)
	items := []string{"34", "25", "9", "1", "23", "12", "22", "201", "128", "134"}
	l.Stable(items)
	for i := 0; i < len(items); i++ {
		fmt.Printf("%v\n", items[i])
	}
	// Output:
	//1
	//9
	//12
	//22
	//23
	//25
	//34
	//128
	//134
	//201
}

func ExampleBytesLessStable() {
	intLess := func(a, b []byte) bool {
		an, _ := strconv.ParseInt(string(a), 10, 64)
		bn, _ := strconv.ParseInt(string(b), 10, 64)
		return an < bn
	}
	l := BytesLess(intLess)
	items := [][]byte{[]byte("34"), []byte("25"), []byte("9"), []byte("1"), []byte("23"),
		[]byte("12"), []byte("22"), []byte("201"), []byte("128"), []byte("134")}
	l.Stable(items)
	for i := 0; i < len(items); i++ {
		fmt.Printf("%v\n", string(items[i]))
	}
	// Output:
	//1
	//9
	//12
	//22
	//23
	//25
	//34
	//128
	//134
	//201
}
func ExampleJSON() {
	var a = `{"name":{"first":"Janet","last":"Prichard"},"age":47}`
	var b = `{"name":{"first":"Tom","last":"Anderson"},"age":39}`
	lastNameLess := func(a, b string) bool {
		ra := gjson.Get(a, "name.last")
		rb := gjson.Get(b, "name.last")
		return ra.String() < rb.String()
	}
	ageLess := func(a, b string) bool {
		ra := gjson.Get(a, "age")
		rb := gjson.Get(b, "age")
		return ra.Int() < rb.Int()
	}
	l := Less(lastNameLess)
	fmt.Printf("%v\n", l.LessThan(a, b))
	fmt.Printf("%v\n", l.GreaterThan(a, b))
	l = Less(ageLess)
	fmt.Printf("%v\n", l.LessThan(a, b))
	fmt.Printf("%v\n", l.GreaterThan(a, b))
	// Output:
	// false
	// true
	// false
	// true
}
