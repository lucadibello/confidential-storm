#!/bin/bash

# This script automatically pulls + rebuilds and install a Teaclave SDK from the patched repo

JAVA_HOME=$GRAAL_VM_HOME
URL=https://github.com/lucadibello/teaclave-java-tee-sdk.git
TEMP_DIR=$(mktemp -d)
DEST_DIR=/opt/javaenclave

set -e

# clone patched sdk from github
git clone --depth 1 --branch master $URL $TEMP_DIR

cd $TEMP_DIR

# build and install teaclave sdk
mkdir -p $DEST_DIR
mvn -f ${TEMP_DIR}/sdk/pom.xml clean install -Pnative -DskipTests
cp -r ${TEMP_DIR}/sdk/native/bin $DEST_DIR; \
cp -r ${TEMP_DIR}/sdk/native/config $DEST_DIR; \
cp -r ${TEMP_DIR}/sdk/native/script/build_app $DEST_DIR; \
rm -rf $TEMP_DIR
echo "Teaclave Java TEE SDK has been successfully installed to $DEST_DIR"

exit 0
