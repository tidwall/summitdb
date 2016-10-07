package machine

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

const (
	clear   = "\x1b[0m"
	bright  = "\x1b[1m"
	dim     = "\x1b[2m"
	black   = "\x1b[30m"
	red     = "\x1b[31m"
	green   = "\x1b[32m"
	yellow  = "\x1b[33m"
	blue    = "\x1b[34m"
	magenta = "\x1b[35m"
	cyan    = "\x1b[36m"
	white   = "\x1b[37m"
)

func TestAll(t *testing.T) {
	mockCleanup()
	defer mockCleanup()

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		mockCleanup()
		os.Exit(1)
	}()

	mc, err := mockOpenCluster(3)
	if err != nil {
		t.Fatal(err)
	}
	defer mc.Close()
	runSubTest(t, "strings", mc, subTestStrings)
}

func runSubTest(t *testing.T, name string, mc *mockCluster, test func(t *testing.T, mc *mockCluster)) {
	t.Run(name, func(t *testing.T) {
		fmt.Printf(bright+"Testing %s\n"+clear, name)
		test(t, mc)
	})
}

func runStep(t *testing.T, mc *mockCluster, name string, step func(mc *mockCluster) error) {
	if err := step(mc); err != nil {
		fmt.Printf("["+red+"fail"+clear+"]: %s\n", name)
		t.Fatal(err)
	}
	fmt.Printf("["+green+"ok"+clear+"]: %s\n", name)
}
