<p align="center">
<img 
    src="resources/logo.png" 
    width="400" height="98" border="0" alt="SummitDB">
</p>

SummitDB is a NoSQL database built on a transactional and strongly-consistent key-value store. It supports ACID transactions, Redis API, custom indexes, JSON documents, geospatial data, user-defined scripting, and more.

The desire was to combine some of the best features of [Redis](http://redis.io) and [MongoDB](https://www.mongodb.com), add strong-consistency and durability, then sprinkle other goodies such as indexing and spatial support. It uses [Finn](https://github.com/tidwall/finn) for the Raft consensus and [BuntDB](https://github.com/tidwall/buntdb) for the datastore.

## Features
- In-memory database for fast reads and writes
- ACID semantics with support for transactions and user-defined scripts
- High-availabilty by implementing the Raft consensus protocol
- Redis API with support for many standard Redis commands
- Key-value store with ordered keyspace
- Flexible iteration of data. ascending, descending, and ranges
- User-defined indexes for various type
- Support for multi value indexes. Similar to a SQL multi column index
- Index fields inside JSON documents. Similar to MongoDB
- Spatial indexing for up to 20 dimensions. Useful for geospatial data
- Support for user-defined scripts using Javascript



===================================

- Getting started

