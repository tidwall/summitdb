package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
	"github.com/tidwall/redlog"
	"github.com/tidwall/summitdb/machine"
)

var version = "0.0.1"

func main() {
	var port int
	var durability string
	var consistency string
	var loglevel string
	var join string
	var dir string
	var high, medium, low bool

	flag.IntVar(&port, "p", 7481, "Bind port")
	flag.StringVar(&durability, "durability", "high", "Log durability [low,medium,high]")
	flag.StringVar(&consistency, "consistency", "high", "Raft consistency [low,medium,high]")
	flag.StringVar(&loglevel, "loglevel", "notice", "Log level [quiet,warning,notice,verbose,debug]")
	flag.StringVar(&dir, "dir", "data", "Data directory")
	flag.StringVar(&join, "join", "", "Join a cluster by providing an address")
	flag.BoolVar(&high, "high", false, "Set durability and consistency to high")
	flag.BoolVar(&medium, "medium", false, "Set durability and consistency to medium")
	flag.BoolVar(&low, "low", false, "Set durability and consistency to low")
	flag.Parse()

	// create a logger that matches the redcon defaults
	log := redlog.New(os.Stderr)

	var opts finn.Options
	opts.Backend = finn.FastLog

	switch strings.ToLower(durability) {
	default:
		log.Warningf("invalid durability '%v'", durability)
		os.Exit(1)
	case "low":
		opts.Durability = finn.Low
	case "medium":
		opts.Durability = finn.Medium
	case "high":
		opts.Durability = finn.High
	}

	switch strings.ToLower(consistency) {
	default:
		log.Warningf("invalid consistency '%v'", consistency)
		os.Exit(1)
	case "low":
		opts.Consistency = finn.Low
	case "medium":
		opts.Consistency = finn.Medium
	case "high":
		opts.Consistency = finn.High
	}
	if low {
		opts.Consistency, opts.Durability = finn.Low, finn.Low
	}
	if medium {
		opts.Consistency, opts.Durability = finn.Medium, finn.Medium
	}
	if high {
		opts.Consistency, opts.Durability = finn.High, finn.High
	}
	switch strings.ToLower(loglevel) {
	default:
		log.Warningf("invalid loglevel '%v'", loglevel)
		os.Exit(1)
	case "quiet":
		opts.LogOutput = ioutil.Discard
	case "warning":
		opts.LogLevel = finn.Warning
	case "notice":
		opts.LogLevel = finn.Notice
	case "verbose":
		opts.LogLevel = finn.Verbose
	case "debug":
		opts.LogLevel = finn.Debug
	}

	addr := fmt.Sprintf(":%d", port)

	// set the log level
	log.SetLevel(int(opts.LogLevel))

	log.Printf("SummitDB %s", version)

	// create the new machine
	m, err := machine.New(log.Sub('M'), addr, dir)
	if err != nil {
		log.Warningf("%v", err)
		os.Exit(1)
	}

	// setup the connection events
	opts.ConnAccept = func(conn redcon.Conn) bool {
		return m.ConnAccept(conn)
	}
	opts.ConnClosed = func(conn redcon.Conn, err error) {
		m.ConnClosed(conn, err)
	}

	// open the raft machine
	n, err := finn.Open(dir, addr, join, m, &opts)
	if err != nil {
		if opts.LogOutput == ioutil.Discard {
			log.Warningf("%v", err)
			os.Exit(1)
		}
		m.Close()
		return
	}
	defer func() {
		n.Close()
		m.Close()
	}()
	// run forever
	select {}
}
