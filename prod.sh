#!/bin/bash -e
if [ -z "$RELEASE" ]; then
    echo "Need to set RELEASE env variable"
    exit 1
fi
echo "Building executable"
CGO_ENABLED=0 go build -a -ldflags '-s -w' -o /build/prj cmd/summitdb-server/main.go
echo "Compressing executable"
./upx --best --lzma /build/prj
echo "Builing docker image"
cat >/build/Dockerfile <<EOL
FROM centurylink/ca-certs
COPY zoneinfo.zip /usr/lib/go/lib/time/zoneinfo.zip
COPY prj /
ENTRYPOINT ["/prj"]
EOL
cd /build
docker build -t pyros2097/summitdb:$RELEASE .
