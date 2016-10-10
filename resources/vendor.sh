#!/bin/bash

set -e

cd $(dirname "${BASH_SOURCE[0]}")/..

if [ "$1" == "" ]; then 
	echo missing argument
else
	mkdir -p vendor/$1 
	cp -rf ${GOPATH}/src/$1/ vendor/$1/
	rm -rf vendor/$1/.git \
		   vendor/$1/.bzr \
		   vendor/$1/.hg \
		   vendor/$1/.svn 
fi
