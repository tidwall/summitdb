<p align="center">
<img 
    src="resources/logo.png" 
    width="350" height="85" border="0" alt="SummitDB">
</p>

SummitDB is a [NoSQL](https://en.wikipedia.org/wiki/NoSQL) database built on a transactional and strongly-consistent key-value store. It using the [Raft](https://raft.github.io/) consensus algorithm, supports [ACID](https://en.wikipedia.org/wiki/ACID) transactions, a [Redis-style API](https://github.com/tidwall/wiki), [custom indexes](https://github.com/tidwall/summitdb/wiki/SETINDEX), [JSON documents](https://github.com/tidwall/summitdb/wiki/SETINDEX#json), [geospatial data](https://github.com/tidwall/summitdb/wiki/SETINDEX#spatial), [user-defined scripting](https://github.com/tidwall/summitdb/wiki/EVAL) using Javascript, and more.

Under the hood it utilizes [Hashicorp Raft](https://github.com/hashicorp/raft), [Finn](https://github.com/tidwall/finn), [Redcon](https://github.com/tidwall/redcon), [BuntDB](https://github.com/tidwall/buntdb), [GJSON](https://github.com/tidwall/gjson), and [Otto](https://github.com/robertkrimen/otto).

## Features

The goal was to create a fast data store that provides:

- In-memory NoSQL solution
- Simplified Redis-style APIs  
- Strong-consistency and durability  
- Ordered key space  
- Indexing on values
- JSON documents
- Spatial indexing

It's a NoSQL solution that is somewhere between Redis and MongoDB, with ACID and high-availablity.

## Getting started

### Building SummitDB

SummitDB can be compiled and used on Linux, OSX, Windows, FreeBSD, and probably others since the codebase is 100% Go. We support both 32 bit and 64 bit systems. Go must be installed on the build machine.

To build simply:

```
$ make
```

It's a good idea to install the [redis-cli](http://redis.io/topics/rediscli).

```
$ make redis-cli
```

To run tests:

````
$ make test
```

### Running

First start a single-member cluster:
```
$ ./summitdb-server
```

This will start the server listening on port 7481 for client and server-to-server communication.

Next, let's set a single key, and then retrieve it:

```
$ ./redis-cli -p 7481 SET mykey "my value"
OK
$ ./redis-cli -p 7481 GET mykey
"my value"
```

Adding members:
```
$ ./summitdb-server -p 7482 -dir data2 -join :7481
$ ./summitdb-server -p 7483 -dir data3 -join :7481
```

That's it. Now if node1 goes down, node2 and node3 will continue to operate.

## Difference between SummitDB and Redis

*It may be worth noting that SummitDB is not a Redis clone. Redis has a lot of commands and data types that not available in SummitDB, such Sets, Hashes, Sorted Sets, and PubSub.*

- **Ordered key space** - SummitDB provides one key space that is a large B-tree. An ordered key space allows for stable paging through keys using the [KEYS](https://github.com/tidwall/summitdb/wiki/KEYS) command. Redis uses an unordered dictionary structure and provides a specialized [SCAN](http://redis.io/commands/scan) command for iterating through keys.
- **Everything a string** - SummitDB stores only strings which are exact binary representations of what the user stores. Redis has many [internal data types](http://redis.io/topics/data-types-intro), such as strings, hashes, floats, sets, etc. 
- **Raft clusters** - SummitDB uses the Raft consensus algorithm to provide high-availablity. Redis provides [Master/Slave replication](http://redis.io/topics/replication). 
- **Javascript** - SummitDB uses Javascript for user-defined scripts. Redis uses Lua.
- **Indexes** - SummitDB provides an API for indexing the key space. Indexes allow for quickly querying and iterating on values. Redis has specialized data types like Sorted Sets and Hashes which can provide [secondary indexing](http://redis.io/topics/indexes).
- **Spatial indexes** - SummitDB provides the ability to create spatial indexes. A spatial index uses an R-tree under the hood, and each index can be up to 20 dimensions. This is useful for geospatial, statistical, time, and range data. Redis has the [GEO API](http://redis.io/commands/geoadd) which allows for using storing and querying geospatial data using the [Geohashes](https://en.wikipedia.org/wiki/Geohash).
- **JSON documents** - SummitDB allows for storing JSON documents and indexing fields directly. Redis has Hashes and a JSON parser via Lua.


## Commands

Below is the complete list of commands and documentation for each.

**Keys and values**  
[APPEND](https://github.com/tidwall/summitdb/wiki/APPEND), 
[BITCOUNT](https://github.com/tidwall/summitdb/wiki/BITCOUNT), 
[BITOP](https://github.com/tidwall/summitdb/wiki/BITOP), 
[BITPOS](https://github.com/tidwall/summitdb/wiki/BITPOS), 
[DBSIZE](https://github.com/tidwall/summitdb/wiki/DBSIZE),
[DECR](https://github.com/tidwall/summitdb/wiki/DECR), 
[DECRBY](https://github.com/tidwall/summitdb/wiki/DECRBY), 
[DEL](https://github.com/tidwall/summitdb/wiki/DEL),
[EXISTS](https://github.com/tidwall/summitdb/wiki/EXISTS),
[EXPIRE](https://github.com/tidwall/summitdb/wiki/EXPIRE),
[EXPIREAT](https://github.com/tidwall/summitdb/wiki/EXPIREAT),
[FLUSHDB](https://github.com/tidwall/summitdb/wiki/FLUSHDB),
[GET](https://github.com/tidwall/summitdb/wiki/GET), 
[GETBIT](https://github.com/tidwall/summitdb/wiki/GETBIT), 
[GETRANGE](https://github.com/tidwall/summitdb/wiki/GETRANGE), 
[GETSET](https://github.com/tidwall/summitdb/wiki/GETSET), 
[INCR](https://github.com/tidwall/summitdb/wiki/INCR), 
[INCRBY](https://github.com/tidwall/summitdb/wiki/INCRBY), 
[INCRBYFLOAT](https://github.com/tidwall/summitdb/wiki/INCRBYFLOAT), 
[KEYS](https://github.com/tidwall/summitdb/wiki/KEYS),
[MGET](https://github.com/tidwall/summitdb/wiki/MGET), 
[MSET](https://github.com/tidwall/summitdb/wiki/MSET), 
[MSETNX](https://github.com/tidwall/summitdb/wiki/MSETNX), 
[PDEL](https://github.com/tidwall/summitdb/wiki/PDEL),
[PERSIST](https://github.com/tidwall/summitdb/wiki/PERSIST),
[PEXPIRE](https://github.com/tidwall/summitdb/wiki/PEXPIRE),
[PEXPIREAT](https://github.com/tidwall/summitdb/wiki/PEXPIREAT),
[PTTL](https://github.com/tidwall/summitdb/wiki/PTTL),
[RENAME](https://github.com/tidwall/summitdb/wiki/RENAME),
[RENAMENX](https://github.com/tidwall/summitdb/wiki/RENAMENX),
[SET](https://github.com/tidwall/summitdb/wiki/SET), 
[SETBIT](https://github.com/tidwall/summitdb/wiki/SETBIT), 
[SETRANGE](https://github.com/tidwall/summitdb/wiki/SETRANGE), 
[STRLEN](https://github.com/tidwall/summitdb/wiki/STRLEN),
[TTL](https://github.com/tidwall/summitdb/wiki/TTL)

**Indexes and iteration**  
[DELINDEX](https://github.com/tidwall/summitdb/wiki/DELINDEX),
[INDEXES](https://github.com/tidwall/summitdb/wiki/INDEXES),
[ITER](https://github.com/tidwall/summitdb/wiki/ITER),
[RECT](https://github.com/tidwall/summitdb/wiki/RECT),
[SETINDEX](https://github.com/tidwall/summitdb/wiki/SETINDEX)

**Transactions**  
[MULTI](https://github.com/tidwall/summitdb/wiki/MULTI),
[EXEC](https://github.com/tidwall/summitdb/wiki/EXEC),
[DISCARD](https://github.com/tidwall/summitdb/wiki/DISCARD)

**Scripts**  
[EVAL](https://github.com/tidwall/summitdb/wiki/EVAL),
[EVALRO](https://github.com/tidwall/summitdb/wiki/EVALRO),
[EVALSHA](https://github.com/tidwall/summitdb/wiki/EVALSHA),
[EVALSHARO](https://github.com/tidwall/summitdb/wiki/EVALSHARO),
[SCRIPT LOAD](https://github.com/tidwall/summitdb/wiki/SCRIPT-LOAD),
[SCRIPT FLUSH](https://github.com/tidwall/summitdb/wiki/SCRIPT-FLUSH)

**Raft management**  
[RAFTADDPEER](https://github.com/tidwall/summitdb/wiki/RAFTADDPEER),
[RAFTREMOVEPEER](https://github.com/tidwall/summitdb/wiki/RAFTREMOVEPEER),
[RAFTLEADER](https://github.com/tidwall/summitdb/wiki/RAFTLEADER),
[RAFTSNAPSHOT](https://github.com/tidwall/summitdb/wiki/RAFTSNAPSHOT),
[RAFTSTATE](https://github.com/tidwall/summitdb/wiki/RAFTSTATE),
[RAFTSTATS](https://github.com/tidwall/summitdb/wiki/RAFTSTATS)

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

SummitDB source code is available under the MIT [License](/LICENSE).



