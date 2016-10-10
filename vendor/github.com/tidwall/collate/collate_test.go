package collate

import (
	"fmt"
	"testing"
)

func TestString(t *testing.T) {
	if len(SupportedLangs()) == 0 {
		t.Fatal("expected something greater than zero")
	}
	less := IndexString("ENGLISH_CI")
	if !less("a", "b") {
		t.Fatal("expected true, got false")
	}
}

func ExampleIndexJSON() {
	var jsonA = `{"name":{"last":"Miller"}}`
	var jsonB = `{"name":{"last":"anderson"}}`
	less := IndexJSON("ENGLISH_CI", "name.last")
	fmt.Printf("%t\n", less(jsonA, jsonB))
	fmt.Printf("%t\n", less(jsonB, jsonA))
	// Output:
	// false
	// true
}

func ExampleIndexString() {
	var nameA = "Miller"
	var nameB = "anderson"
	less := IndexString("ENGLISH_CI")
	fmt.Printf("%t\n", less(nameA, nameB))
	fmt.Printf("%t\n", less(nameB, nameA))
	// Output:
	// false
	// true
}

func ExampleSpanish() {
	less := IndexString("SPANISH_CI")
	fmt.Printf("%t\n", less("Hola", "hola"))
	fmt.Printf("%t\n", less("hola", "Hola"))
	// Output:
	// false
	// false
}
