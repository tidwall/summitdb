LESS
====
![Travis CI Build Status](https://api.travis-ci.org/tidwall/less.svg?branch=master)
[![GoDoc](https://godoc.org/github.com/tidwall/less?status.svg)](https://godoc.org/github.com/tidwall/less)

Less is a super simple library that provides relational comparisons for string-based [less functions](#less-functions).

Getting Started
===============

## Installing

To start using the less package, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/less
```

Less Functions
==============
A less function is one that takes two params and returns true if the first param is less than the second.

For example:

```go
func simpleLess(a, b string) bool{
	return a < b
}
```

Or perhaps for case insensitive comparisons:

```go
func ciLess(a, b string) bool{
	return strings.ToLower(a) < strings.ToLower(b)
}
```

Maybe you want to compare two ints:

```go
func intLess(a, b string) bool{
	an, _ := strconv.ParseInt(a, 10, 64)
	bn, _ := strconv.ParseInt(b, 10, 64)
	return an < bn
}
```

A more complicated example could be with comparing fields inside two JSON documents.
In this example we'll use the [GJSON](https://github.com/tidwall/gjson) package.

Using these two documents:

```go
var a = `{"name":{"first":"Janet","last":"Prichard"},"age":47}`
var b = `{"name":{"first":"Tom","last":"Anderson"},"age":39}`
```

We want to compare based on age and last name:

```go
func ageLess func(a, b string) bool {
	ra := gjson.Get(a, "age")
	rb := gjson.Get(b, "age")
	return ra.Int() < rb.Int()
}
func lastNameLess func(a, b string) bool {
	ra := gjson.Get(a, "name.last")
	rb := gjson.Get(b, "name.last")
	return ra.String() < rb.String()
}
```

Using
=====

This library provide a bunch of handy functions for relational comparisons such as:

```
LessThan             returns true for "a < b"
LessThanOrEqualTo    returns true for "a <= b"
GreaterThan          returns true for "a > b"
GreaterThanOrEqualTo returns true for "a >= b"
EqualTo              returns true for "a == b"
Sort                 sorts data.
Stable               sorts data while keeping the original order of equal elements.
```

For example, let's say we want to compare two integers represented as strings.

```go
// intLess returns true when "a < b"
intLess := func(a, b string) bool{
	an, _ := strconv.ParseInt(a, 10, 64)
	bn, _ := strconv.ParseInt(b, 10, 64)
	return an < bn
}

l := less.Less(intLess)  // wrap the less function

// compare two numbers
l.GreaterThan("12", "2") // returns true 

// sort an array 
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
```

Comparing fields from JSON documents using the [GJSON](https://github.com/tidwall/gjson) package.

```go
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

lastName := less.Less(lastNameLess)
fmt.Printf("%v\n", lastName.LessThan(a, b))
fmt.Printf("%v\n", lastName.GreaterThan(a, b))

age := less.Less(ageLess)
fmt.Printf("%v\n", age.LessThan(a, b))
fmt.Printf("%v\n", age.GreaterThan(a, b))

// Output:
// false
// true
// false
// true
```

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

LESS source code is available under the MIT [License](/LICENSE).
