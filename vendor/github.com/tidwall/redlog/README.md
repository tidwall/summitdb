Redlog
======
<a href="https://travis-ci.org/tidwall/redlog"><img src="https://img.shields.io/travis/tidwall/redlog.svg?style=flat-square" alt="Build Status"></a>
<a href="https://godoc.org/github.com/tidwall/redlog"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>

Redlog is a [Redis style logger](http://build47.com/redis-log-format-levels/) for Go.

Installing
----------

```
go get -u github.com/tidwall/redlog
```

Example
-------

```go
log := redlog.New(os.Stderr)
log.Printf("Server started at 10.0.1.5:6379")
log.Debugf("Connected to leader")
log.Warningf("Heartbeat timeout reached, starting election")
```

Output:

```
[93324:M] 29 Aug 09:30:59.943 * Server started at 10.0.1.5:6379
[93324:M] 29 Aug 09:31:01.892 . Connected to leader
[93324:M] 29 Aug 09:31:02.331 # Heartbeat timeout reached, starting election 
```

Contact
-------
Josh Baker [@tidwall](http://twitter.com/tidwall)

License
-------
Redcon source code is available under the MIT [License](/LICENSE).
