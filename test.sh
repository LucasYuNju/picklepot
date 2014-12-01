#!/bin/sh

#prepare data
echo "\nPreparing data"
rm -r tpch-dbgen
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make -s 2>>logs
./dbgen -s 1
cd ..
cp tpch-dbgen/orders.tbl data/

#build and test
export MAVEN_OPTS="-Xms1g -Xmx4g"
BUILD_COMMAND="mvn -Dtest=TestPerf test"
echo "\nTesting with..."
echo "\$ $BUILD_COMMAND\n"

${BUILD_COMMAND}

