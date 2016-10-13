all:
	@NOOP=`go version` && \
	TMP=`mktemp -d -t sdb.XXXX` && \
	mkdir -p $$TMP/src/github.com/tidwall/ && \
	cp -rf . $$TMP/src/github.com/tidwall/summitdb && \
	pushd $$TMP/src/github.com/tidwall/summitdb > /dev/null && \
	GOPATH=$$TMP/ make build && \
	popd > /dev/null && \
	cp -rf $$TMP/src/github.com/tidwall/summitdb/summitdb-server . && \
	rm -rf $$TMP/
build: 
	@go build -ldflags "-X main.version=0.2.1" -o summitdb-server cmd/summitdb-server/main.go 
clean:
	@rm -f summitdb-server
	@rm -rf redis-3.2.4
	@rm -f redis-cli
.PHONY: test
test:
	@cd machine && go test 
install: all
	@cp summitdb-server /usr/local/bin
uninstall: 
	@rm -f /usr/local/bin/summitdb-server
redis-cli:
	@rm -rf redis-3.2.4 && \
	curl http://download.redis.io/releases/redis-3.2.4.tar.gz | tar xz && \
	cd redis-3.2.4 && \
	make redis-cli && \
	cp src/redis-cli .. && \
	cd .. && \
	rm -rf redis-3.2.4
