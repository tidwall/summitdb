all: 
	@resources/build.sh
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
package:
	@NOCOPY=1 resources/build.sh package
