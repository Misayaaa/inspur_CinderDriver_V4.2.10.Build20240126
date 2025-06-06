#!/bin/bash

CINDER_CONF=/etc/cinder/cinder.conf

if [ ! -f $CINDER_CONF ]
then
    echo ">>>>>[INFO] Not a cinder service node."
    exit 0
fi

# Be carefully when add new option to the content.
# All lines should be ended with '\n\'.
CINDER_BACKEND_NAME=@@STORAGE_BACKEND_NAME@@
CINDER_CONTENT="\
###COMMENT-BY-AUTOCONF-TOOL START CONFIG $CINDER_BACKEND_NAME\n\
[$CINDER_BACKEND_NAME]\n\
volume_driver = @@VOLUME_DRIVER@@\n\
san_ip = @@STORAGE_IP@@\n\
san_login = @@STORAGE_USERNAME@@\n\
san_password = @@STORAGE_PASSWORD@@\n\
instorage_mcs_volpool_name = @@STORAGE_POOL_NAME@@\n\
volume_backend_name = $CINDER_BACKEND_NAME\n\
use_multipath_for_image_xfer = True\n\
image_volume_cache_enabled = True\n\
image_volume_cache_max_size_gb = 2000\n\
image_volume_cache_max_count = 500\n\
###COMMENT-BY-AUTOCONF-TOOL END CONFIG $CINDER_BACKEND_NAME
"

backendStartLine=$(sed -n "/###COMMENT-BY-AUTOCONF-TOOL START CONFIG $CINDER_BACKEND_NAME/=" $CINDER_CONF)
backendEndLine=$(sed -n "/###COMMENT-BY-AUTOCONF-TOOL END CONFIG $CINDER_BACKEND_NAME/=" $CINDER_CONF)
# Delete the original configure.
if [[ $backendStartLine != "" && $backendEndLine != "" ]]
then
  sed -i "$backendStartLine,$backendEndLine d" $CINDER_CONF 
fi
# Add the new configure, just append to the last.
sed -i "\$a $CINDER_CONTENT" $CINDER_CONF

# Set enabled_backends.
backend_names=`egrep '^enabled_backends' /etc/cinder/cinder.conf |tail -1|tr '=,' "\n"`
for name in $backend_names;do
  if [[ $CINDER_BACKEND_NAME == $name ]];then
    exit 0
  fi
done
sed -i "s/^enabled_backends.*=.*/&,$CINDER_BACKEND_NAME/" $CINDER_CONF


