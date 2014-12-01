#!/bin/sh

#prepare data
echo "\nPreparing data"
rm -r tpch-dbgen
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
make -s 2>>logs
./dbgen -s 1
cd ..
mkdir data
cp tpch-dbgen/orders.tbl data/

#build and test
export MAVEN_OPTS="-Xms1024m -Xmx4096m"
BUILD_COMMAND="mvn -Dtest=TestPerf test"
echo "\nTesting with..."
echo "\$ $BUILD_COMMAND\n"

${BUILD_COMMAND}

