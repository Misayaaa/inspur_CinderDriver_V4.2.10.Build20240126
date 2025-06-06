#!/bin/bash

cinderCount=$(systemctl list-units | grep -c openstack-cinder-volume.service)
if [[ $cinderCount -ne 0 ]]
then
    echo ">>>>>[INFO] Restart openstack cinder api service."
    systemctl restart openstack-cinder-api.service
    echo ">>>>>[INFO] Restart openstack cinder scheduler service."
    systemctl restart openstack-cinder-scheduler.service
    echo ">>>>>[INFO] Restart openstack cinder volume service."
    systemctl restart openstack-cinder-volume.service
fi

glanceCount=$(systemctl list-units | grep -c openstack-glance-api.service)
if [[ $glanceCount -ne 0 ]]
then
    echo ">>>>>[INFO] Restart openstack glance api service."
    systemctl restart openstack-glance-api.service
fi

novaCount=$(systemctl list-units | grep -c openstack-nova-compute.service)
if [[ $novaCount -ne 0 ]]
then
    echo ">>>>>[INFO] Restart openstack nova compute service."
    systemctl restart openstack-nova-compute.service
fi
