<p align="center">
<img 
    src="resources/logo.png" 
    width="400" height="98" border="0" alt="SummitDB">
</p>

SummitDB is a NoSQL database built on a transactional and strongly-consistent key-value store. It supports ACID transactions, [Redis-like API](#commands), custom indexes, JSON documents, geospatial data, user-defined scripting, and more.

The desire was to combine some of the best features of [Redis](http://redis.io) and [MongoDB](https://www.mongodb.com), add strong-consistency and durability, then sprinkle other goodies such as indexing and spatial support. It uses [Finn](https://github.com/tidwall/finn) for the Raft consensus and [BuntDB](https://github.com/tidwall/buntdb) for the datastore.

## Features
- In-memory database for fast reads and writes
- ACID semantics with support for transactions and user-defined scripts
- High-availabilty by implementing the Raft consensus protocol
- Redis-like API with support for many standard Redis commands
- Key-value store with ordered keyspace
- Flexible iteration of data. ascending, descending, and ranges
- User-defined indexes for various type
- Support for multi value indexes. Similar to a SQL multi column index
- Index fields inside JSON documents. Similar to MongoDB
- Spatial indexing for up to 20 dimensions. Useful for geospatial data
- Support for user-defined scripts using Javascript

[test markdown](/resources/test.md#test-markdown

===================================

- Getting started

## Commands

SummitDB supports come common Redis commands and adds many more. Here's a complete list of commands:

append
bitcount
bitop
bitpos
exec
expire
expireat
dbsize
decr
decrby
del
delindex
discard
dump
eval
evalro
evalsha
evalsharo
exists
flushdb
flushall
get
getbit
getrange
getset
incr
incrby
incrbyfloat
indexes
iter
keys
mget
multi
mset
msetnx
pdel
persist
pexpire
pexpireat
psetex
pttl
raftaddpeer
raftremovepeer
raftleader
raftsnapshot
raftstate
raftstats
rect
rename
renamenx
restore
script
set
setbit
setex
setindex
setnx
setrange
strlen
ttl
time
type

