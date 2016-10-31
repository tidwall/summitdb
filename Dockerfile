FROM alpine:3.4

RUN apk add --no-cache git bash vim ca-certificates openssl docker go nodejs

ENV GOPATH /go
RUN mkdir /pkg
