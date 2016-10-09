all: 
	@go build -ldflags "-X main.version=0.2.1" -o summitdb-server cmd/summitdb-server/main.go 
clean:
	@rm -f summitdb-server
.PHONY: test
test:
	@cd machine && go test 
install: all
	@cp summitdb-server /usr/local/bin
uninstall: 
	@rm -f /usr/local/bin/summitdb-server

