<p align="center">
<img 
    src="logo.jpg" 
    width="314" height="200" border="0" alt="FINN">
</p>
<p align="center">
<a href="https://travis-ci.org/tidwall/finn"><img src="https://img.shields.io/travis/tidwall/finn.svg?style=flat-square" alt="Build Status"></a>
<a href="https://goreportcard.com/report/github.com/tidwall/finn"><img src="https://goreportcard.com/badge/github.com/tidwall/finn?style=flat-square" alt="Go Report Card"></a>
<a href="https://godoc.org/github.com/tidwall/finn"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>



Finn is a fast and simple framework for building [Raft](https://raft.github.io/) implementations in Go. It uses [Redcon](https://github.com/tidwall/redcon) for the network transport and [Hashicorp Raft](https://github.com/hashicorp/raft). There is also the option to use [BoltDB](https://github.com/boltdb/bolt) or [FastLog](https://github.com/tidwall/raft-fastlog) for log persistence.

The reason for this project is to add Raft support to a future release of [BuntDB](https://github.com/tidwall/buntdb) and [Tile38](https://github.com/tidwall/tile38).

Features
--------

- Simple API for quickly creating a [fault-tolerant](https://en.wikipedia.org/wiki/Fault_tolerance) cluster
- Fast network protocol using the [raft-redcon](https://github.com/tidwall/raft-redcon) transport
- Optional [backends](#log-backends) for log persistence. [BoltDB](https://github.com/boltdb/bolt) or [FastLog](https://github.com/tidwall/raft-fastlog)
- Adjustable [consistency and durability](#consistency-and-durability) levels
- A [full-featured example](#full-featured-example) to help jumpstart integration
- [Built-in raft commands](#built-in-raft-commands) for monitoring and managing the cluster
- Supports the [Redis log format](http://build47.com/redis-log-format-levels/)
- Works with clients such as [redigo](https://github.com/garyburd/redigo), [redis-py](https://github.com/andymccurdy/redis-py), [node_redis](https://github.com/NodeRedis/node_redis), [jedis](https://github.com/xetorthio/jedis), and [redis-cli](http://redis.io/topics/rediscli)


Getting Started
---------------

### Installing

To start using Finn, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/finn
```

This will retrieve the library.

### Example

Here's an example of a Redis clone that accepts the GET, SET, DEL, and KEYS commands.

You can run a [full-featured version](#full-featured-example) of this example from a terminal:

```
go run example/clone.go
```

```go
package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/tidwall/finn"
	"github.com/tidwall/match"
	"github.com/tidwall/redcon"
)

func main() {
	n, err := finn.Open("data", ":7481", "", NewClone(), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer n.Close()
	select {}
}

type Clone struct {
	mu   sync.RWMutex
	keys map[string][]byte
}

func NewClone() *Clone {
	return &Clone{keys: make(map[string][]byte)}
}

func (kvm *Clone) Command(m finn.Applier, conn redcon.Conn, cmd redcon.Command) (interface{}, error) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		return nil, finn.ErrUnknownCommand
	case "set":
		if len(cmd.Args) != 3 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				kvm.mu.Lock()
				kvm.keys[string(cmd.Args[1])] = cmd.Args[2]
				kvm.mu.Unlock()
				return nil, nil
			},
			func(v interface{}) (interface{}, error) {
				conn.WriteString("OK")
				return nil, nil
			},
		)
	case "get":
		if len(cmd.Args) != 2 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd, nil,
			func(interface{}) (interface{}, error) {
				kvm.mu.RLock()
				val, ok := kvm.keys[string(cmd.Args[1])]
				kvm.mu.RUnlock()
				if !ok {
					conn.WriteNull()
				} else {
					conn.WriteBulk(val)
				}
				return nil, nil
			},
		)
	case "del":
		if len(cmd.Args) < 2 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		return m.Apply(conn, cmd,
			func() (interface{}, error) {
				var n int
				kvm.mu.Lock()
				for i := 1; i < len(cmd.Args); i++ {
					key := string(cmd.Args[i])
					if _, ok := kvm.keys[key]; ok {
						delete(kvm.keys, key)
						n++
					}
				}
				kvm.mu.Unlock()
				return n, nil
			},
			func(v interface{}) (interface{}, error) {
				n := v.(int)
				conn.WriteInt(n)
				return nil, nil
			},
		)
	case "keys":
		if len(cmd.Args) != 2 {
			return nil, finn.ErrWrongNumberOfArguments
		}
		pattern := string(cmd.Args[1])
		return m.Apply(conn, cmd, nil,
			func(v interface{}) (interface{}, error) {
				var keys []string
				kvm.mu.RLock()
				for key := range kvm.keys {
					if match.Match(key, pattern) {
						keys = append(keys, key)
					}
				}
				kvm.mu.RUnlock()
				sort.Strings(keys)
				conn.WriteArray(len(keys))
				for _, key := range keys {
					conn.WriteBulkString(key)
				}
				return nil, nil
			},
		)
	}
}

func (kvm *Clone) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	data, err := ioutil.ReadAll(rd)
	if err != nil {
		return err
	}
	var keys map[string][]byte
	if err := json.Unmarshal(data, &keys); err != nil {
		return err
	}
	kvm.keys = keys
	return nil
}

func (kvm *Clone) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	data, err := json.Marshal(kvm.keys)
	if err != nil {
		return err
	}
	if _, err := wr.Write(data); err != nil {
		return err
	}
	return nil
}
```

The Applier Type
----------------
Every `Command()` call provides an `Applier` type which is responsible for handling all Read or Write operation. In the above example you will see one `m.Apply(conn, cmd, ...)` for each command.

The signature for the `Apply()` function is:
```go
func Apply(
	conn redcon.Conn, 
	cmd redcon.Command,
	mutate func() (interface{}, error),
	respond func(interface{}) (interface{}, error),
) (interface{}, error)
```

- `conn` is the client connection making the call. It's possible that this value may be `nil` for commands that are being replicated on Follower nodes. 
- `cmd` is the command to process.
- `mutate` is the function that handles modifying the node's data. 
Passing `nil` indicates that the operation is read-only.
The `interface{}` return value will be passed to the `respond` func.
Returning an error will cancel the operation and the error will be returned to the client.
- `respond` is used for responding to the client connection. It's also used for read-only operations. The `interface{}` param is what was passed from the `mutate` function and may be `nil`. 
Returning an error will cancel the operation and the error will be returned to the client.

*Please note that the `Apply()` command is required for modifying or accessing data that is shared on all of the nodes.
Optionally you can forgo the call altogether for operations that are unique to the node.*

Snapshots
---------
All Raft commands are stored in one big log file that will continue to grow. The log is stored on disk, in memory, or both. At some point the server will run out of memory or disk space.
Snapshots allows for truncating the log so that it does not take up all of the server's resources.

The two functions `Snapshot` and `Restore` are used to create a snapshot and restore a snapshot, respectively.

The `Snapshot()` function passes a writer that you can write your snapshot to.
Return `nil` to indicate that you are done writing. Returning an error will cancel the snapshot. If you want to disable snapshots altogether:

```go
func (kvm *Clone) Snapshot(wr io.Writer) error {
	return finn.ErrDisabled
}
```

The `Restore()` function passes a reader that you can use to restore your snapshot from.

*Please note that the Raft cluster is active during a snapshot operation. 
In the example above we use a read-lock that will force the cluster to delay all writes until the snapshot is complete.
This may not be ideal for your scenario.*

Full-featured Example
---------------------

There's a command line Redis clone that supports all of Finn's features. Print the help options:

```
go run example/clone.go -h
```

First start a single-member cluster:
```
go run example/clone.go
```

This will start the clone listening on port 7481 for client and server-to-server communication.

Next, let's set a single key, and then retrieve it:

```
$ redis-cli -p 7481 SET mykey "my value"
OK
$ redis-cli -p 7481 GET mykey
"my value"
```

Adding members:
```
go run example/clone.go -p 7482 -dir data2 -join 7481
go run example/clone.go -p 7483 -dir data3 -join 7481
```

That's it. Now if node1 goes down, node2 and node3 will continue to operate.


Built-in Raft Commands
----------------------
Here are a few commands for monitoring and managing the cluster:

- **RAFTADDPEER addr**  
Adds a new member to the Raft cluster
- **RAFTREMOVEPEER addr**  
Removes an existing member
- **RAFTPEERS addr**  
Lists known peers and their status
- **RAFTLEADER**  
Returns the Raft leader, if known
- **RAFTSNAPSHOT**  
Triggers a snapshot operation
- **RAFTSTATE**  
Returns the state of the node
- **RAFTSTATS**  
Returns information and statistics for the node and cluster

Consistency and Durability
--------------------------

### Write Durability

The `Options.Durability` field has the following options:

- `Low` - fsync is managed by the operating system, less safe
- `Medium` - fsync every second, fast and safer
- `High` - fsync after every write, very durable, slower

### Read Consistency

The `Options.Consistency` field has the following options:

- `Low` - all nodes accept reads, small risk of [stale](http://stackoverflow.com/questions/1563319/what-is-stale-state) data
- `Medium` - only the leader accepts reads, itty-bitty risk of stale data during a leadership change
- `High` - only the leader accepts reads, the raft log index is incremented to guaranteeing no stale data

For example, setting the following options:

```go
opts := finn.Options{
	Consistency: High,
	Durability: High,
}
n, err := finn.Open("data", ":7481", "", &opts)
```

Provides the highest level of durability and consistency.

Log Backends
------------
Finn supports the following log databases.

- [FastLog](https://github.com/tidwall/raft-fastlog) - log is stored in memory and persists to disk, fast, log is limited to the amount of server memory.
- [Bolt](https://github.com/boltdb/bolt) - log is stored only to disk, slower, supports larger logs.

Contact
-------
Josh Baker [@tidwall](http://twitter.com/tidwall)

License
-------
Finn source code is available under the MIT [License](/LICENSE).
