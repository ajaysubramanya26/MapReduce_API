#! /bin/bash

VERSION=`uname`
rm -rf MasterIp
#Launch Mac specific version
if [ "$VERSION" == "Darwin" ]; then
	./MacLinux.sh
fi
#launch Linux (Ubuntu) specific version
if [ "$VERSION" == "Linux" ]; then
	./UbuntuLinux.sh
fi
