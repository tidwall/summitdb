#!/bin/bash
set -e

VERSION="0.2.2"
PROTECTED_MODE="no"

export GO15VENDOREXPERIMENT=1

cd $(dirname "${BASH_SOURCE[0]}")
OD="$(pwd)"

# temp directory for storing isolated environment.
TMP="$(mktemp -d -t sdb.XXXX)"
function rmtemp {
	rm -rf "$TMP"
}
trap rmtemp EXIT

if [ "$NOCOPY" != "1" ]; then
	# copy all files to an isloated directory.
	WD="$TMP/src/github.com/tidwall/summitdb"
	export GOPATH="$TMP"
	for file in `find . -type f`; do
		# TODO: use .gitignore to ignore, or possibly just use git to determine the file list.
		if [[ "$file" != "." && "$file" != ./.git* && "$file" != ./data* && "$file" != ./summitdb-* ]]; then
			mkdir -p "$WD/$(dirname "${file}")"
			cp -P "$file" "$WD/$(dirname "${file}")"
		fi
	done
	cd $WD
fi

# build and store objects into original directory.
go build -ldflags "-X main.version=$VERSION" -o "$OD/summitdb-server" cmd/summitdb-server/*.go

