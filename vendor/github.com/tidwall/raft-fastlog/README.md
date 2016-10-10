raft-fastlog
===========

This repository provides the `raftfastlog` package. 
The package exports a Raft Store which is an implementation of a
`LogStore`, `StableStore`, and `PeerStore`.

It is meant to be used as a backend for the `raft` 
[package here](https://github.com/hashicorp/raft).

This implementation is an in-memory database that persists to disk.

RaftStore Performance Comparison
--------------------------------

FastLog (This implementation)
```
BenchmarkBuntStore_FirstIndex-8    20000000           92 ns/op
BenchmarkBuntStore_LastIndex-8     20000000           92 ns/op
BenchmarkBuntStore_GetLog-8        10000000          139 ns/op
BenchmarkBuntStore_StoreLog-8       1000000         2028 ns/op
BenchmarkBuntStore_StoreLogs-8       300000         4507 ns/op
BenchmarkBuntStore_DeleteRange-8    1000000         3164 ns/op
BenchmarkBuntStore_Set-8            1000000         1522 ns/op
BenchmarkBuntStore_Get-8           10000000          119 ns/op
BenchmarkBuntStore_SetUint64-8      1000000         1506 ns/op
BenchmarkBuntStore_GetUint64-8     20000000          117 ns/op
```

[MDB](https://github.com/hashicorp/raft-mdb)
```
BenchmarkMDBStore_FirstIndex-8  	 500000	        3043 ns/op
BenchmarkMDBStore_LastIndex-8  	     500000	        2941 ns/op
BenchmarkMDBStore_GetLog-8     	     300000	        4665 ns/op
BenchmarkMDBStore_StoreLog-8   	      10000	      183860 ns/op
BenchmarkMDBStore_StoreLogs-8  	      10000	      193783 ns/op
BenchmarkMDBStore_DeleteRange-8	      10000	      199927 ns/op
BenchmarkMDBStore_Set-8        	      10000	      147540 ns/op
BenchmarkMDBStore_Get-8        	     500000	        2324 ns/op
BenchmarkMDBStore_SetUint64-8  	      10000	      162291 ns/op
BenchmarkMDBStore_GetUint64-8  	    1000000	        2451 ns/op
```

[BoltDB](https://github.com/hashicorp/raft-boltdb)
```
BenchmarkBoltStore_FirstIndex-8 	2000000 	     848 ns/op
BenchmarkBoltStore_LastIndex-8  	2000000	         857 ns/op
BenchmarkBoltStore_GetLog-8     	 500000	        3169 ns/op
BenchmarkBoltStore_StoreLog-8   	  10000	      197432 ns/op
BenchmarkBoltStore_StoreLogs-8  	  10000	      205238 ns/op
BenchmarkBoltStore_DeleteRange-8	  10000	      189994 ns/op
BenchmarkBoltStore_Set-8        	  10000	      177010 ns/op
BenchmarkBoltStore_Get-8        	2000000	         983 ns/op
BenchmarkBoltStore_SetUint64-8  	  10000	      175435 ns/op
BenchmarkBoltStore_GetUint64-8  	2000000	         976 ns/op
```
