#!/bin/bash

DATA_PATH=$1
if [[ $DATA_PATH == "" ]]
then
    DATA_PATH=./data
fi

USE_ID_RSA=""
if [ -f $DATA_PATH/openstack_id_rsa ]
then
    USE_ID_RSA="-i $DATA_PATH/openstack_id_rsa"
fi

if [ -f $DATA_PATH/setting ]
then
    source $DATA_PATH/setting
fi

RUN_PATH=/root/as18000-cinder-auto-config-data

# Refresh the tools.
rm -f $DATA_PATH/*.sh
rm -f $DATA_PATH/README.md

cp tools/*.sh $DATA_PATH/

# Update the configuration to setting value.
CHECK_RUN_ENV=$DATA_PATH/check_run_env.sh
OPENSTACK_KEYSTONRC_PATH=${OPENSTACK_KEYSTONRC_PATH:-"/root/keystonerc_admin"}
sed -i "s|@@KEYSTONRC_ADMIN_PATH@@|$OPENSTACK_KEYSTONRC_PATH|" $CHECK_RUN_ENV

# build scp ip for both ipv4 and ipv6
function build_scp_ip()
{
    SCP_IP=$1
    if [[ $SCP_IP =~ .*:.* ]]
    then
       SCP_IP="[${SCP_IP}]"
    fi
    echo $SCP_IP
}

# Copy data to all openstack node
for node in $OPENSTACK_CONTROLLER_NODES
do
    echo ">>>>>[INFO] Clean the work directory on $node."
    ssh $USE_ID_RSA root@$node "rm -rf $RUN_PATH && mkdir -p $RUN_PATH"
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Clean work directory on $node failed."
        exit 1
    fi

    echo ">>>>>[INFO] Copy datas to work directory on $node."
    node_ip=`build_scp_ip $node`
    scp $USE_ID_RSA -r $DATA_PATH/*.sh root@$node_ip:$RUN_PATH
    ssh $USE_ID_RSA root@$node_ip chmod -R 755 $RUN_PATH
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Copy datas to work directory on $node failed."
        exit 1
    fi
done

# Run the check job on each openstack node, to make sure the env is ok.
for node in $OPENSTACK_CONTROLLER_NODES
do
    echo ">>>>>[INFO] Check openstack service on $node."
    ssh $USE_ID_RSA root@$node $RUN_PATH/check_run_env.sh
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Check openstack service on $node failed."
        exit 1
    fi
done
