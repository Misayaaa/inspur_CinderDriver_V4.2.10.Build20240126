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

"""
Volume driver for inspur.
"""

from cinder.i18n import _
from cinder.volume import driver
from cinder import exception
# the openstack common package is move out from project
# as some independent library since Kilo
try:
    # the configuration is work since Kilo
    from oslo_config import cfg
    from oslo_log import log as logging
except ImportError:
    # the configuration is work before Juno(including Juno)
    from cinder.openstack.common import log as logging
    from oslo.config import cfg

LOG = logging.getLogger(__name__)


instorage_mcs_opts = [
    cfg.StrOpt('san_ip',
               default='',
               help='IP address of SAN controller'),
    cfg.StrOpt('san_login',
               default='admin',
               help='Username for SAN controller'),
    cfg.StrOpt('san_password',
               default='',
               help='Password for SAN controller',
               secret=True),
    cfg.StrOpt('san_private_key',
               default='',
               help='Filename of private key to use for SSH authentication'),
    cfg.PortOpt('san_ssh_port',
                default=22,
                help='SSH port to use with SAN'),
    cfg.PortOpt('san_api_port',
                help='Port to use to access the SAN API'),
    cfg.BoolOpt('san_is_local',
                default=False,
                help='Execute commands locally instead of over SSH; '
                     'use if the volume service is running on the SAN device'),
    cfg.IntOpt('ssh_conn_timeout',
               default=30,
               help="SSH connection timeout in seconds"),
    cfg.IntOpt('ssh_min_pool_conn',
               default=1,
               help='Minimum ssh connections in the pool'),
    cfg.IntOpt('ssh_max_pool_conn',
               default=5,
               help='Maximum ssh connections in the pool'),
    cfg.BoolOpt('instorage_mcs_vol_autoexpand',
                default=True,
                help='Storage system autoexpand parameter for volumes '
                     '(True/False)'),
    cfg.BoolOpt('instorage_mcs_vol_compression',
                default=False,
                help='Storage system compression option for volumes'),
    cfg.BoolOpt('instorage_mcs_vol_deduplicate',
                default=False,
                help='Storage system deduplicate option for volumes'),
    cfg.BoolOpt('instorage_mcs_vol_intier',
                default=True,
                help='Enable InTier for volumes'),
    cfg.BoolOpt('instorage_mcs_allow_tenant_qos',
                default=False,
                help='Allow tenants to specify QOS on create'),
    cfg.BoolOpt('instorage_mcs_vol_nofmtdisk',
                default=False,
                help='Specifies that the volume not be formatted during '
                     'creation.'),
    cfg.IntOpt('instorage_mcs_vol_grainsize',
               default=256,
               help='Storage system grain size parameter for volumes '
                    '(32/64/128/256)'),
    cfg.IntOpt('instorage_mcs_vol_rsize',
               default=-1,
               # min=-1, max=100,
               help='Storage system space-efficiency parameter for volumes '
                    '[-1 - 100](percentage)'),
    cfg.IntOpt('instorage_mcs_vol_warning',
               default=80,
               # min=-1, max=100,
               help='Storage system threshold for volume capacity warnings '
                    '[-1 - 100] (percentage)'),
    cfg.IntOpt('instorage_mcs_localcopy_timeout',
               default=120,
               # min=1, max=600,
               help='Maximum number of seconds to wait for LocalCopy to be '
                    'prepared. [1 - 600]'),
    cfg.IntOpt('instorage_mcs_localcopy_rate',
               default=100,
               # min=1, max=150,
               help='Specifies the InStorage LocalCopy copy rate to be used '
               'when creating a full volume copy. The default is rate '
               'is 100, and the valid rates are 1-150.'),
    cfg.IntOpt('instorage_mcs_localcopy_clean_rate',
               default=100,
               help='Specifies the InStorage LocalCopy clean rate to be used '
               'when creating a full volume copy. The default is rate '
               'is 100, and the valid rates are 1-150.'),
    cfg.StrOpt('instorage_mcs_vol_iogrp',
               default='0',
               help='The I/O group in which to allocate volumes. It can be a '
               'comma-separated list in which case the driver will select an '
               'io_group based on least number of volumes associated with the '
               'io_group.'),
    cfg.StrOpt('instorage_mcs_vol_access_iogrp',
               default=None,
               help='Specifies the members of the volume I/O group access set.'),
    cfg.StrOpt('instorage_mcs_stretched_cluster_partner',
               default=None,
               help='If operating in stretched cluster mode, specify the '
                    'name of the pool in which mirrored copies are stored.'
                    'Example: "pool2"'),
    cfg.ListOpt('instorage_mcs_volpool_name',
                default=['volpool'],
                help='Comma separated list of storage system storage '
                     'pools for volumes.'),
    cfg.StrOpt('instorage_san_secondary_ip',
               default=None,
               help='Specifies secondary management IP or hostname to be '
                    'used if san_ip is invalid or becomes inaccessible.'),
    cfg.BoolOpt('instorage_mcs_enable_aa',
                default=False,
                help='Create active-active volume when do volume create'),
    cfg.ListOpt('instorage_mcs_aa_volpool_name_map',
                default=None,
                help='Comma separated list of storage system storage '
                     'pools map for active-active volumes. Each item is '
                     'a base pool to second pool map, seperated by :, '
                     'like Pool0:Pool1, and Pool0 should be a argument to '
                     'instorage_mcs_volpool_name'),
    cfg.ListOpt('instorage_mcs_aa_iogrp_map',
                default=None,
                help='Comma separated list of storage system I/O group '
                     'map for active-active volumes. Each item is '
                     'a base iogrp to second iogrp map, seperated by :, '
                     'like iogrp0:iogrp1, and iogrp0 should be a argument to '
                     'instorage_mcs_vol_iogrp'),
    cfg.BoolOpt('instorage_mcs_auto_delete_host',
                default=True,
                help='Auto delete the host when no volume mapped to it.'),
    cfg.IntOpt('instorage_mcs_syncrate',
               default=80,
               # min=1, max=100,
               help='Specifies the InStorage sync rate to be used '
               'when creating a volume for formatting. The default is rate '
               'is 80, and the valid rates are 1-100.'),
    cfg.IntOpt('instorage_mcs_loop_check_interval',
               default=60,
               # min=1, max=600,
               help='Maximum number of seconds to wait between each check.'),
    cfg.StrOpt('instorage_mcs_supervisor_mode',
               default="SSH",
               help='the mode of managing storage, support SSH, In-band, Rest'),
    cfg.BoolOpt('instorage_mcs_check_root_cert',
                default=False,
                help='Whether do the root cert check.'),
    cfg.StrOpt('instorage_mcs_ca_root',
                help='The ca root path of the as18000.'),
    cfg.BoolOpt('instorage_mcs_check_client_cert',
                default=False,
                help='Whether do the connection cert check.'),
    cfg.StrOpt('instorage_mcs_ca_key',
                help='The path of the connection cert key.'),
    cfg.StrOpt('instorage_mcs_ca_cert',
                help='The path of the connection cert file.'),
    cfg.IntOpt('instorage_mcs_host_identifier',
               # min=0, max=1023, should be unique
               default=-1,
               help='controller node identifier.'),
    cfg.StrOpt('instorage_mcs_cluster_id',
               default=None,
               help='cluster identifier.'),
    cfg.StrOpt('instorage_rsa_private_key',
               default='',
               help='RSA private key for decrypt.'),
    cfg.IntOpt('instorage_mcs_thread_num',
               default=10,
               help='thread num for rest'),
    cfg.IntOpt('ssh_exec_timeout',
               default=60,
               help="SSH execute timeout in seconds"),
    cfg.BoolOpt('migration_waiting_for_copy',
                default=True,
                help='volume migration waiting for data copy, '
                     'true means waiting for volume to copy in storage, '
                     'and updating the status of volume after the copy is completed.'
                     'false means not waiting for volume to be copied in storage. '
                ),
    cfg.BoolOpt('icos_support',
               default=False,
               help="Whether support ICOS"),
    cfg.BoolOpt('instorage_mcs_fastconsistgrp_support',
               default=False,
               help="create group using fast localcopy consistgrp or not"),
]

instorage_mcs_iscsi_opts = [
    cfg.BoolOpt('instorage_mcs_iscsi_chap_enabled',
                default=False,
                help='Configure CHAP authentication for iSCSI connections '
                     '(Default: False)'),
    cfg.ListOpt('instorage_mcs_iscsi_ips',
                default=[],
                help='attach a volume using spcific iscsi ips'),
]

instorage_mcs_fc_opts = [
    cfg.ListOpt('instorage_mcs_fc_wwpns',
                default=[],
                help='attach a volume using spcific FC port')
]

instorage_mcs_roce_opts = [
    cfg.ListOpt('instorage_mcs_rdma_ips',
                default=[],
                help='attach a volume using spcific rdma ips')
]

as13000_base_opts = [
    cfg.StrOpt('san_ip',
               default='',
               help='IP address of SAN controller'),
    cfg.StrOpt('san_login',
               default='admin',
               help='Username for SAN controller'),
    cfg.StrOpt('san_password',
               default='',
               help='Password for SAN controller',
               secret=True),
    cfg.ListOpt(
        'as13000_ipsan_pools',
        default=['Pool0'],
        help='The Storage Pools Cinder should use, a comma separated list.'),
    cfg.StrOpt(
        'as13000_dev_username',
        required=True,
        help='The device username of the storage system.'),
    cfg.StrOpt(
        'as13000_dev_password',
        required=True,
        secret=True,
        help='The device password of the storage system.'),
    cfg.BoolOpt(
        'as13000_check_root_cert',
        default=False,
        help='Whether do the root cert check.'),
    cfg.StrOpt(
        'as13000_ca_root',
        help='The ca root path of the as13000.'),
    cfg.BoolOpt(
        'as13000_check_client_cert',
        default=False,
        help='Whether do the connection cert check.'),
    cfg.StrOpt(
        'as13000_ca_key',
        help='The path of the connection cert key.'),
    cfg.StrOpt(
        'as13000_ca_cert',
        help='The path of the connection cert file.'),
    cfg.StrOpt(
        'as13000_rsa_private_key',
        required=True,
        default='',
        help='RSA private key for decrypt.'),
    cfg.BoolOpt(
        'as13000_flatten_volume_from_snapshot',
        default=False,
        help='Flatten volumes created from snapshots to remove '
        'dependency from volume to snapshot'),
    cfg.BoolOpt(
        'as13000_cluster_stat',
        default=False,
        help='Report cluster information when get_volume_stats '),
    cfg.IntOpt(
        'as13000_max_clone_depth',
        default=0,
        help='The user has the option to limit how long a volumes clone '
             'chain can be by setting as13000_max_clone_depth. If a clone '
             'is made of another clone and that clone has '
             'as13000_max_clone_depth clones behind it, the source '
             'volume will be flattened.'),
]

as13000_iscsi_opts = [
    cfg.DictOpt(
        'as13000_ip_translate_table',
        default={},
        help='A translate table for storage ip, like ipa:ip1,ipb:ip2 .'),
    cfg.StrOpt(
        'as13000_iscsi_domain_name',
        help='The domain name used as iSCSI portal. '
        'When set, it will be used instead of the node ip.'),
    cfg.IntOpt(
        'as13000_api_version',
        default=2,
        help='Version of the as13000 API to use (integer value)'),
    cfg.IntOpt(
        'as13000_iscsi_volume_attach_api_version',
        default=2,
        help='Version of the as13000 API to use (integer value)'),
    cfg.BoolOpt(
        'use_ipv6',
        help='Use IPv6 for iSCSI connection'),
]

as13000_rbd_opts = [
    cfg.ListOpt(
        'as13000_mon_addrs',
        default=[],
        help='The as13000 mon addr, a comma separated list.'),
    cfg.StrOpt(
        'as13000_rbd_user',
        help='The rbd device access username for the storage system.'),
    cfg.StrOpt(
        'as13000_rbd_secret_uuid',
        help='The rbd secret uuid used in the libvirt system.'),
    cfg.StrOpt(
        'as13000_rbd_cluster_name',
        help='The cluster name of the storage system.'),
    cfg.StrOpt(
        'as13000_rbd_conf',
        help='The rbd configuration file used to access storage system.'),
    cfg.IntOpt(
        'as13000_rbd_store_chunk_size',
        default=4,
        help='The rbd chunk size when create a new volume, unit is MB.'),
    cfg.IntOpt('as13000_rados_connection_interval', default=5,
               help=_('Interval value (in seconds) between connection '
                      'retries to ceph cluster.')),
    cfg.IntOpt('as13000_rados_connection_retries', default=3,
               help=_('Number of retries if connection to ceph cluster '
                      'failed.')),
    cfg.IntOpt(
        'as13000_rbd_connect_timeout',
        default=300,
        help='The rbd connect timetout, unit is second.'),
]

CONF = cfg.CONF


class InBaseDriver(driver.VolumeDriver):
    VENDOR = 'inspur'
    VERSION = '5.0.0.1'
    CI_WIKI_NAME = 'inspur_CI'

    def __init__(self, *args, **kwargs):
        super(InBaseDriver, self).__init__(*args, **kwargs)

        if not self.configuration:
            msg = _('Configuration is not found.')
            LOG.error(msg)
            raise exception.InvalidInput(reason=msg)

    # ATTENTION the signature of create_export change from L, which add
    # the connector argument, we set it to None as default so that J, K
    # can also use it
    def create_export(self, ctxt, volume, connector=None):
        pass

    def remove_export(self, ctxt, volume):
        pass

    def create_export_snapshot(self, context, snapshot, connector=None):
        pass

    def remove_export_snapshot(self, context, snapshot):
        pass

    def ensure_export(self, context, volume):
        """Synchronously recreates an export for a logical volume."""
        pass
