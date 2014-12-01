#!/bin/sh

#prepare data
if [ ! -f "data/orders.tbl" ]
then
    TPCH="tpch-dbgen"
    echo "\nPreparing data"
    rm -rf $TPCH
    git clone https://github.com/electrum/tpch-dbgen.git
    cd $TPCH
    make -s 2>>logs
    ./dbgen -s 1
    cd ..
    mkdir data
    mv $TPCH/orders.tbl data/
fi

#build and test
BUILD_COMMAND="mvn -Dtest=TestPerf test"
echo "\nTesting with..."
echo "\$ $BUILD_COMMAND\n"

${BUILD_COMMAND}
