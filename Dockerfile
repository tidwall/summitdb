FROM alpine:3.4

RUN apk add --no-cache git bash vim ca-certificates openssl docker go nodejs

COPY zoneinfo.zip /build/zoneinfo.zip
COPY upx /bin/upx

ENV GOPATH /go
RUN mkdir /pkg
