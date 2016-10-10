package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	var dims int
	var debug bool
	flag.IntVar(&dims, "dims", 4, "number of dimensions")
	flag.BoolVar(&debug, "debug", false, "turn on debug tracing")
	flag.Parse()
	// process rtree.go
	data, err := ioutil.ReadFile("src/rtree.go")
	if err != nil {
		log.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "// +build ignore", "// generated; DO NOT EDIT!", -1))
	if debug {
		data = []byte(strings.Replace(string(data), "TDEBUG", "true", -1))
	} else {
		data = []byte(strings.Replace(string(data), "TDEBUG", "false", -1))
	}
	var dimouts = make([]string, dims)
	var output string
	var recording bool
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			idx := strings.Index(line, "//")
			switch strings.ToUpper(strings.TrimSpace(line[idx+2:])) {
			case "BEGIN":
				recording = true
				for i := 0; i < len(dimouts); i++ {
					dimouts[i] = ""
				}
				continue
			case "END":
				for _, out := range dimouts {
					if out != "" {
						output += out
					}
				}
				recording = false
				continue
			}
		}
		if recording {
			for i := 0; i < len(dimouts); i++ {
				dimouts[i] += strings.Replace(line, "TNUMDIMS", strconv.FormatInt(int64(i+1), 10), -1) + "\n"
			}
		} else {
			output += line + "\n"
		}
	}
	// process rtree_base.go
	if err := os.RemoveAll("../dims"); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < dims; i++ {
		sdim := strconv.FormatInt(int64(i+1), 10)
		data, err := ioutil.ReadFile("src/rtree_base.go")
		if err != nil {
			log.Fatal(err)
		}
		data = []byte(strings.Split(string(data), "// FILE_START")[1])
		if debug {
			data = []byte(strings.Replace(string(data), "TDEBUG", "true", -1))
		} else {
			data = []byte(strings.Replace(string(data), "TDEBUG", "false", -1))
		}
		data = []byte(strings.Replace(string(data), "TNUMDIMS", strconv.FormatInt(int64(i+1), 10), -1))
		data = []byte(strings.Replace(string(data), "DD_", "d"+strconv.FormatInt(int64(i+1), 10), -1))
		if err := os.MkdirAll("../dims/d"+sdim, 0777); err != nil {
			log.Fatal(err)
		}
		output = string(append([]byte(output), data...))
	}
	if err := ioutil.WriteFile("../rtree.go", []byte(output), 0666); err != nil {
		log.Fatal(err)
	}
}
