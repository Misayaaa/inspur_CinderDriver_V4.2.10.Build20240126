#!/bin/bash

OPTION=$1
SUFFIX=$2
PY2_VERSION="python2.[0-9]"
PY3_VERSION="python3.[0-9]"
PY_CINDER_VERSION=""
PY2_CINDER_DIR="/usr/lib/$PY2_VERSION/site-packages/cinder/volume/"
PY3_CINDER_DIR="/usr/lib/$PY3_VERSION/site-packages/cinder/volume/"
if [[ $SUFFIX == "" ]]
then
    SUFFIX=$(date +%m%d%H%M)
fi

if [[ $OPTION == "backup" ]]
then
    OPERATION="cp -af"
    FROM_SUFFIX=""
    TO_SUFFIX="_BACKUP-BY-AUTOCONF-TOOL_$SUFFIX"
elif [[ $OPTION == "restore" ]]
then
    OPERATION="cp -af"
    FROM_SUFFIX="_BACKUP-BY-AUTOCONF-TOOL_$SUFFIX"
    TO_SUFFIX=""
elif [[ $OPTION == "cleanup" ]]
then
    OPERATION="rm -f"
    FROM_SUFFIX="_BACKUP-BY-AUTOCONF-TOOL_$SUFFIX"
    TO_SUFFIX="_BACKUP-BY-AUTOCONF-TOOL_$SUFFIX"
else
    echo ">>>>>[ERROR] $OPTION invalid, can only be (backup, restore, cleanup)."
    exit 0
fi

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

TARGET_DIRS=""
TARGET_DIRS="$TARGET_DIRS /etc/cinder/cinder.conf"
TARGET_DIRS="$TARGET_DIRS /usr/lib/${PY_CINDER_VERSION}/site-packages/cinder/volume/drivers/inspur"
for dir in $TARGET_DIRS
do
    if [ -e ${dir}${FROM_SUFFIX} ]
    then
        echo ">>>>>[INFO] $OPTION ${dir}${FROM_SUFFIX}"
        if [[ $OPTION == "restore" ]]
        then
            rm -rf ${dir}${TO_SUFFIX}
        fi
        $OPERATION -r ${dir}${FROM_SUFFIX} ${dir}${TO_SUFFIX}
    else
        echo ">>>>>[WARNING] the backup file ${dir}${FROM_SUFFIX} is not found"
        exit 1
    fi
done
