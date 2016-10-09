all: 
	@go build -o mercdb-server cmd/merc-server/main.go 
clean:
	@rm -f mercdb-server
.PHONY: test
test:
	@cd machine && go test 
install: all
	@cp mercdb-server /usr/local/bin
uninstall: 
	@rm -f /usr/local/bin/mercdb-server

