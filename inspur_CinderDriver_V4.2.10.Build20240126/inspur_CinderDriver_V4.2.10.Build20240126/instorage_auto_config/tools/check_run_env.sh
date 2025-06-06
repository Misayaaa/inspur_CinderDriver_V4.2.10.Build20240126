#!/bin/bash

KEYSTONRC_ADMIN_PATH=@@KEYSTONRC_ADMIN_PATH@@
if [[ -f $KEYSTONRC_ADMIN_PATH ]]
then
    echo ">>>>>[INFO] This node can do administrate operation, check service."
    source $KEYSTONRC_ADMIN_PATH
    #echo "Get openstack service list as follows:"
    #openstack service list

    echo ">>>>>[INFO] Get openstack nova service list as follows:"
    nova service-list
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Access to nova api service failed, please repair it."
    else
        downsNova=$(nova service-list | grep -c '|[[:blank:]]*down[[:blank:]*|]')
        if [[ $downsNova -ne 0 ]]
        then
            echo ">>>>>[WARNING] $downsNova nova service down, please pay attention."
        fi
    fi

    echo ">>>>>[INFO] Get openstack neutron agent list as follows:"
    neutron agent-list -f table -c "agent_type" -c "host" -c "alive"
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Access to neutron api service failed, please repair it."
    else
        notAlives=$(neutron agent-list -f table -c "agent_type" -c "host" -c "alive" | grep -c -v ':-)')
        if [[ $notAlives -ne 4 ]]
        then
            echo ">>>>>[WARNING] $(($notAlives - 4)) neutron agents no alive, please pay attention."
        fi
    fi

    echo ">>>>>[INFO] Get cinder service list as follows:"
    cinder service-list
    if [[ $? -ne 0 ]]
    then
        echo ">>>>>[ERROR] Access to cinder api service failed, please repair it."
    else
        downServices=$(cinder service-list | grep -c '|[[:blank:]]*down[[:blank:]*|]')
        if [[ $downServices -ne 0 ]]
        then
            echo ">>>>>[WARNING] ${downServices} cinder service down, please pay attention."
        fi
    fi

    echo ">>>>>[INFO] List available glance images as follows:"
    glance image-list
else
    echo ">>>>>[INFO] This node can not do openstack administrate operation."
fi
