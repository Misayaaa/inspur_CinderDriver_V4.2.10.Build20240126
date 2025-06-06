#!/bin/bash
WORKDIR=$(dirname $0)
cd $WORKDIR

driver=$(find . -name "inspur_CinderDriver_*")
if [[ $driver = "" ]]
then
    echo ">>>>>[WARNING] Driver file not exist, do nothing."
    exit 1
fi

tar -xzvf $driver

driverDir=${driver/'.tar.gz'/}
cd $driverDir 

PY2_VERSION="python2.[0-9]"
PY3_VERSION="python3.[0-9]"
PY2_CINDER_DIR="/usr/lib/$PY2_VERSION/site-packages/cinder/volume/"
PY3_CINDER_DIR="/usr/lib/$PY3_VERSION/site-packages/cinder/volume/"

function judge_python_version() {

    if [[ $1 == "cinder" ]]
    then
        if [ -d $PY3_CINDER_DIR ];then
            echo "$PY3_VERSION"
            exit 0
        elif [ -d $PY2_CINDER_DIR ];then
            echo "$PY2_VERSION"
            exit 0
        fi
    else
        echo "$1 not a valid param."
        exit 1
    fi

    echo ""
    exit 0
}

PY_CINDER_VERSION=$(judge_python_version cinder)


CINDER_DRIVER_DIR="/usr/lib/$PY_CINDER_VERSION/site-packages/cinder/volume/drivers"
SOURCE_CINDER_DIR="cinder/volume/drivers/inspur"

if [ -d $CINDER_DRIVER_DIR ]
then
    cp -rf $SOURCE_CINDER_DIR $CINDER_DRIVER_DIR
    echo "Install instorage cinder driver complete."
else
    echo ">>>>>[RRROR] Cinder driver install: no drivers folder found."
    exit 1
fi



