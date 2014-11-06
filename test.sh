#!/bin/sh

BUILD_COMMAND="mvn -Dtest=TestPerf test"

echo "\nTesting with..."
echo "\$ $BUILD_COMMAND\n"

${BUILD_COMMAND}

#rm -r target
