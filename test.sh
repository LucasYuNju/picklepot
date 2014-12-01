#!/bin/sh

#prepare data
if [ ! -f "data/orders.tbl" ]
then
    echo "\nPreparing data"
    rm -rf tpch-dbgen
    git clone https://github.com/electrum/tpch-dbgen.git
    cd tpch-dbgen
    make -s 2>>logs
    ./dbgen -s 1
    cd ..
    mkdir data
    cp tpch-dbgen/orders.tbl data/
fi

#build and test
BUILD_COMMAND="mvn -Dtest=TestPerf test"
echo "\nTesting with..."
echo "\$ $BUILD_COMMAND\n"

${BUILD_COMMAND}
