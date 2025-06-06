# Copyright 2017 inspur Corp.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

DEV_MODEL_INSTORAGE = '1813'
DEV_MODEL_INSTORAGE_AS5X00 = '2076'

REP_CAP_DEVS = (DEV_MODEL_INSTORAGE, DEV_MODEL_INSTORAGE_AS5X00)

# constants used for replication
ASYNC = 'async'
SYNC = 'sync'
VALID_REP_TYPES = (ASYNC, SYNC)
FAILBACK_VALUE = 'default'

DEFAULT_RC_TIMEOUT = 3600 * 24 * 7
DEFAULT_RC_INTERVAL = 5

INTERVAL_1_SEC = 1
DEFAULT_TIMEOUT = 20

REPLICA_AUX_VOL_PREFIX = 'aux_'

# remote mirror copy status
REP_CONSIS_SYNC = 'consistent_synchronized'
REP_CONSIS_STOP = 'consistent_stopped'
REP_CONSIS_COPYING = 'consistent_copying'
REP_SYNC = 'synchronized'
REP_IDL = 'idling'
REP_IDL_DISC = 'idling_disconnected'
REP_STATUS_ON_LINE = 'online'

REPLICA_AUX_VOL_SUFFIX = ''

# constants used for support version of iscsi volume chain clone
VERSION_SUPPORT_ISCSI_VOLUME_CHAIN_CLONE = 'V3.7.41.5'

# constants used for support version of iscsi volume attach api 2,
# one REST method for attach/detach iscsi volume
VERSION_SUPPORT_ISCSI_VOLUME_ATTACH_API_VERSION_2 = 'V3.7.41.6'

# QoS limit: 1)BANDWIDTH:100KB/s~10GB/s; 2)IOPS:10~1000000
MIN_BANDWIDTH = 1024 * 100
MAX_BANDWIDTH = 10 * 1024 * 1024 * 1024
MIN_IOPS = 10
MAX_IOPS = 1000000

# Ignore the failed rest response, go through as success.
FAIL_HANDLER_IGNORE = 'ignore'
# Raise exception for the failed rest response.
FAIL_HANDLER_RAISE = 'raise'

MAX_TARGET_NUM = 64  # max target number that  support
MAX_LUN_NUM_PER_TARGET = 64  # max lun number that  support per target
OPTIONAL_TARGET_NUM_FOR_CINDER = 32  # advice target number that cinder use
CINDER_TARGET_PREFIX = 'target.inspur.cinder-'  # cinder target name prefix