#!/bin/bash

# This script automatically pulls + rebuilds and install a Teaclave SDK from the patched repo

URL=https://github.com/lucadibello/teaclave-java-tee-sdk.git
TEMP_DIR=$(mktemp -d)
DEST_DIR=/opt/javaenclave

# ensure graalvm is installed
if [ -z "$GRAALVM_HOME" ]; then
    echo "GRAALVM_HOME is not set. Please install GraalVM and set GRAALVM_HOME environment variable."
    exit 1
fi

if [ ! -d "$GRAALVM_HOME" ]; then
    echo "GRAALVM_HOME directory does not exist: $GRAALVM_HOME"
    exit 1
fi

if [ ! -f "$GRAALVM_HOME/bin/java" ]; then
    echo "GraalVM java binary not found in GRAALVM_HOME: $GRAALVM_HOME/bin/java"
    exit 1
fi

set -e

# clone patched sdk from github
git clone --branch master --depth 1 https://github.com/lucadibello/teaclave-java-tee-sdk.git /tmp/teaclave-java-tee-sdk

# Install GraalVM processor as Maven artifact
/usr/local/bin/mvn install:install-file \
    -DgroupId=org.graalvm.compiler \
    -DartifactId=graal-processor \
    -Dversion=22.2.0 \
    -Dpackaging=jar \
    -Dfile="${GRAALVM_HOME}/lib/graal/graal-processor.jar"

# Pre-download all dependencies and plugins
/usr/local/bin/mvn -f /tmp/teaclave-java-tee-sdk/sdk/pom.xml \
    dependency:resolve \
    dependency:resolve-plugins

# Build and install teaclave sdk
mkdir -p /opt/javaenclave
mvn -f /tmp/teaclave-java-tee-sdk/sdk/pom.xml clean install -Pnative -DskipTests
cp -r /tmp/teaclave-java-tee-sdk/sdk/native/bin /opt/javaenclave
cp -r /tmp/teaclave-java-tee-sdk/sdk/native/config /opt/javaenclave
cp -r /tmp/teaclave-java-tee-sdk/sdk/native/script/build_app /opt/javaenclave
rm -rf /tmp/teaclave-java-tee-sdk
echo "Teaclave Java TEE SDK has been successfully installed to /opt/javaenclave"

exit 0
