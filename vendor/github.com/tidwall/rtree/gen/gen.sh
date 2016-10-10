#!/bin/bash

set -e

cd $(dirname "${BASH_SOURCE[0]}")

go run gen.go --dims=20 --debug=false 
cd ..
go fmt
