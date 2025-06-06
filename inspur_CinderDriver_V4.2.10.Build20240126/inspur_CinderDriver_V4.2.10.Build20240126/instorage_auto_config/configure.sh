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
ADD_CINDER_BACKEND=$DATA_PATH/add_cinder_backend.sh


# Refresh the tools.
rm -f $DATA_PATH/*.sh
rm -f $DATA_PATH/README.md
cp tools/*.sh $DATA_PATH/

sed -i "s/@@STORAGE_IP@@/$STORAGE_IP/" $ADD_CINDER_BACKEND  
sed -i "s/@@STORAGE_USERNAME@@/$STORAGE_USERNAME/" $ADD_CINDER_BACKEND  
sed -i "s#@@STORAGE_PASSWORD@@#$STORAGE_PASSWORD#" $ADD_CINDER_BACKEND  
sed -i "s/@@STORAGE_POOL_NAME@@/$STORAGE_POOL_NAME/" $ADD_CINDER_BACKEND  
sed -i "s#@@STORAGE_BACKEND_NAME@@#$STORAGE_BACKEND_NAME#" $ADD_CINDER_BACKEND
link_type_to_uppercase=`echo ${STORAGE_PROTOCOL}|tr 'a-z' 'A-Z'`

if [[ $link_type_to_uppercase == ISCSI ]]
then
    sed -i "s/@@VOLUME_DRIVER@@/cinder.volume.drivers.inspur.instorage.instorage_iscsi.InStorageMCSISCSIDriver/" $ADD_CINDER_BACKEND 
elif [[ $link_type_to_uppercase == FC ]]
then 
    sed -i "s/@@VOLUME_DRIVER@@/cinder.volume.drivers.inspur.instorage.instorage_fc.InStorageMCSFCDriver/" $ADD_CINDER_BACKEND
elif [[ $link_type_to_uppercase == RDMA ]]
then
    sed -i "s/@@VOLUME_DRIVER@@/cinder.volume.drivers.inspur.instorage.instorage_rdma.InStorageMCSRDMADriver/" $ADD_CINDER_BACKEND
else
    echo -e "protocol $link_type_to_uppercase is not supported,Please check setting. STORAGE_PROTOCOL must be ISCSI or FC or RDMA"
    exit 1
fi

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
    scp $USE_ID_RSA -r $DATA_PATH/* root@$node_ip:$RUN_PATH
    ssh $USE_ID_RSA root@$node_ip chmod -R 755 $RUN_PATH
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Copy datas to work directory on $node failed."
        exit 1
    fi
done

JOBS=""
JOBS="$JOBS add_cinder_backend.sh"
JOBS="$JOBS install_cinder_driver.sh"
JOBS="$JOBS restart_openstack_service.sh"

# Run the configure job on each node.
for node in $OPENSTACK_CONTROLLER_NODES
do
    for job in $JOBS
    do
        echo ">>>>>[INFO] Do $job on $node."
        ssh $USE_ID_RSA root@$node $RUN_PATH/$job
        if [[ $? -ne 0 ]]
        then
            echo ">>>>>[ERROR] do $job on $node failed."
            exit 1
        fi
    done
done
