#!/bin/bash
# This script will setup the cinder driver and the configuration for community OpenStack
ERROR_COLOR_BEGIN="\033[31m"
ERROR_COLOR_END="\033[0m"
DATA_PATH=$1
if [[ $DATA_PATH == "" ]]
then
    DATA_PATH=./data
fi

if [ -f $DATA_PATH/setting ]
then
    source $DATA_PATH/setting
fi

link_type_to_uppercase=`echo ${STORAGE_PROTOCOL}|tr 'a-z' 'A-Z'`
if [[ ${link_type_to_uppercase} != 'ISCSI' && ${link_type_to_uppercase} != 'FC' && ${link_type_to_uppercase} != 'RDMA' ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_PROTOCOL must be ISCSI or FC or RDMA${ERROR_COLOR_END}"
    exit
fi

if [[ -z ${STORAGE_IP} ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_IP is empty${ERROR_COLOR_END}"
    exit
fi          
ipcalc -c4 ${STORAGE_IP} &>/dev/null
ipv4_check_result=$?
ipcalc -c6 ${STORAGE_IP} &>/dev/null
ipv6_check_result=$?
if [[ ${ipv4_check_result} -ne 0 && ${ipv6_check_result} -ne 0 ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_IP ${STORAGE_IP} format is wrong${ERROR_COLOR_END}"
    exit
fi
     
if [[ -z ${STORAGE_USERNAME} ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_USERNAME is empty${ERROR_COLOR_END}"
    exit
fi
     
if [[ -z ${STORAGE_PASSWORD} ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_PASSWORD is empty${ERROR_COLOR_END}"
    exit
fi
     
if [[ -z ${STORAGE_POOL_NAME} ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_POOL_NAME is empty${ERROR_COLOR_END}"
    exit
fi
if [[ ! ${STORAGE_POOL_NAME} =~ ^[a-zA-Z0-9]+[,]*[a-zA-Z0-9]+$ ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_POOL_NAME is invailed. If more than one pools should be configed, please split by comma${ERROR_COLOR_END}"
    exit
fi

if [[ -z ${STORAGE_BACKEND_NAME} ]];then
    echo -e "${ERROR_COLOR_BEGIN}Please check setting. STORAGE_BACKEND_NAME is empty${ERROR_COLOR_END}"
    exit
fi

ping -c4 ${STORAGE_IP} &>/dev/null
if [ ! $? -eq 0 ];then
    echo -e "${ERROR_COLOR_BEGIN}san_ip ${STORAGE_IP} is not reachable${ERROR_COLOR_END}"
    exit
fi
#check the storage pool
storage_pool_array=(`echo $STORAGE_POOL_NAME|tr ',' ' '`)
for var in ${storage_pool_array[@]};do 
    echo "check the stroage pool ${var} on ${STORAGE_IP}, please input the storage password"
    cmd="mcsinq lsmdiskgrp ${var}"
    ssh ${STORAGE_USERNAME}@${STORAGE_IP} $cmd &>/dev/null
    if [ ! $? -eq 0 ];then
        echo -e "${ERROR_COLOR_BEGIN}storage pool ${var} is not exist${ERROR_COLOR_END}"
        exit
    fi
done
