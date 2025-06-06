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

import math
import random
import re
import time
import socket
import threading
import paramiko
import six

from eventlet import greenthread
from cinder import context
from cinder import exception
from cinder.i18n import _
from cinder import ssh_utils
from cinder import utils as cinder_utils
from cinder.message import api as message_api
from cinder.message import message_field
from cinder.volume import volume_types

from cinder.volume.drivers.inspur import base
from cinder.volume.drivers.inspur import constants as instorage_const
from cinder.volume.drivers.inspur.utils import RsaUtil
from cinder.volume.drivers.inspur import utils as as18000_utils
from cinder.volume.drivers.inspur.instorage import (
    instorage_replication as instorage_rep)
from cinder.volume.drivers.inspur.instorage.instorage_inband import (
     RestExecutor, InStorageRestAssistant)
from cinder.volume.drivers.inspur.instorage import instorage_cli

# !!!ATTENTION
# the openstack common package is move out from project
# as some independent library from Kilo
try:
    # the config is work as oslo more early
    from oslo.config import cfg
    from cinder.openstack.common import excutils
    from cinder.openstack.common import processutils
    from cinder.openstack.common import log as logging
    from cinder.openstack.common import jsonutils as json
    from cinder.openstack.common import strutils
    from cinder.openstack.common import units
except ImportError:
    from oslo_config import cfg
    from oslo_concurrency import processutils
    from oslo_log import log as logging
    from oslo_serialization import jsonutils as json
    from oslo_utils import excutils
    from oslo_utils import strutils
    from oslo_utils import units

try:
    from cinder.openstack.common import loopingcall
except ImportError:
    from oslo_service import loopingcall

# !!!ATTENTION
# from Train, openstack rename utils in cinder/volume as volume_utils
try:
    from cinder.volume import utils as volume_utils
except ImportError:
    from cinder.volume import volume_utils


LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(base.instorage_mcs_opts)

log_filter = as18000_utils.LogFilter
utils_trace = as18000_utils.inspur_trace


class InStorageMCSCommonDriver(base.InBaseDriver):
    """inspur InStorage MCS abstract base class for iSCSI/FC volume drivers."""

    VERSION = "4.2.10"
    VENDOR = 'inspur'
    CI_WIKI_NAME = "inspur_CI"

    VDISKCOPYOPS_INTERVAL = 60

    def __init__(self, *args, **kwargs):
        super(InStorageMCSCommonDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(base.instorage_mcs_opts)
        self._backend_name = self.configuration.safe_get('volume_backend_name')
        self.active_ip = self.configuration.san_ip
        self.inactive_ip = self.configuration.instorage_san_secondary_ip

        self.message_api = message_api.API()
        self.active_active_pool_map = {}
        pool_maps = self.configuration.instorage_mcs_aa_volpool_name_map
        if pool_maps:
            for pool_map in pool_maps:
                primary, second = tuple(pool_map.split(':'))
                self.active_active_pool_map[primary] = second
                self.active_active_pool_map[second] = primary
            LOG.debug("aa pool map: %s", self.active_active_pool_map)

        self.active_active_iogrp_map = {}
        iogrp_maps = self.configuration.instorage_mcs_aa_iogrp_map
        if iogrp_maps:
            for iogrp_map in iogrp_maps:
                primary, second = tuple(iogrp_map.split(':'))
                self.active_active_iogrp_map[primary] = second
                self.active_active_iogrp_map[second] = primary
            LOG.debug("aa iogrp map: %s", self.active_active_iogrp_map)

        self.loop_check_interval = self.configuration.instorage_mcs_loop_check_interval
        self.sshpool = None
        self.sshpool_available = True
        self.supervisor_mode = self.configuration.instorage_mcs_supervisor_mode
        self.port = self.configuration.safe_get('san_api_port')
        self.ca_root = self.configuration.instorage_mcs_ca_root
        self.ca_key = self.configuration.instorage_mcs_ca_key
        self.ca_cert = self.configuration.instorage_mcs_ca_cert
        self.use_ssl = self.configuration.safe_get('driver_use_ssl') or False
        self.check_root_cert = self.configuration.instorage_mcs_check_root_cert
        self.check_client_cert = self.configuration.instorage_mcs_check_client_cert
        self.host_identifier = self.configuration.safe_get('instorage_mcs_host_identifier')
        self.cluster_id = self.configuration.safe_get('instorage_mcs_cluster_id')
        self._local_backend_rest = None
        self._rest = self._local_backend_rest
        self.rsa_util = None
        self.icos_support = self.configuration.icos_support
        self.request_semaphore = threading.Semaphore(self.configuration.ssh_max_pool_conn)

        if self.supervisor_mode == 'In-band':
            required_flags = ['instorage_mcs_host_identifier', 'instorage_mcs_cluster_id']
            for flag in required_flags:
                if self.configuration.safe_get(flag) == -1:
                    raise exception.InvalidInput(reason=_('%s is not set.') % flag)
                if self.configuration.safe_get(flag) is None:
                    raise exception.InvalidInput(reason=_('%s is not set.') % flag)

        if not self.port:
            self.port = '8446' if self.use_ssl else '8443'

        if self.supervisor_mode == 'SSH':
            assistant = instorage_cli.InStorageAssistant(self._run_ssh,
                                       self.active_active_pool_map,
                                       self.active_active_iogrp_map,
                                       self.loop_check_interval)
        elif self.supervisor_mode == 'Rest' or self.supervisor_mode == 'In-band':
            assistant = InStorageRestAssistant(self._run_rest,
                                       self.active_active_pool_map,
                                       self.active_active_iogrp_map,
                                       self.loop_check_interval)
        if self.supervisor_mode == 'In-band':
            assistant.clear_scsi_cache()

        self._local_backend_assistant = assistant
        self._aux_backend_assistant = None
        self._assistant = self._local_backend_assistant
        self._vdiskcopyops = {}
        self._vdiskcopyops_loop = None
        self.protocol = None
        self.replication = None
        self._state = {'storage_nodes': {},
                       'enabled_protocols': set(),
                       'compression_enabled': False,
                       'deduped_enabled': False,
                       'available_iogrps': [],
                       'system_name': None,
                       'system_id': None,
                       'code_level': None,
                       'cloned_extend_support':False,
                       'fastconsistgrp_support':False
                       }
        self._active_backend_id = kwargs.get('active_backend_id')

        # This dictionary is used to map each replication target to certain
        # replication manager object.
        self.replica_manager = {}

        # One driver can be configured with only one replication target
        # to failover.
        self._replica_target = {}

        # This boolean is used to indicate whether replication is supported
        # by this storage.
        self._replica_enabled = False

        # This list is used to save the supported replication modes.
        self._supported_replica_types = []

        # This is used to save the available pools in failed-over status
        self._secondary_pools = None

        # InStorage has the limitation that can not burst more than 3 new ssh
        # connections within 1 second. So slow down the initialization.
        time.sleep(1)

    def do_setup(self, ctxt):
        """Check that we have all configuration details from the storage."""
        LOG.debug('enter: do_setup')

        # Update the instorage state
        self._update_instorage_state()

        # v2.1 replication setup
        self._get_instorage_config()

        # Validate that the pool exists
        self._validate_pools_exist()

        LOG.debug('leave: do_setup')

    def _update_instorage_state(self):
        # Get storage system name, id, and code level
        self._state.update(self._assistant.get_system_info())

        capabilities_value = self._assistant.get_capabilities_value()

        # Check if compression is supported
        self._state['compression_enabled'] = (self._assistant.
                                              compression_enabled())
        # Check if deduped is supported
        self._state['deduped_enabled'] = (self._assistant.
                                          deduped_enabled())

        # Get the available I/O groups
        self._state['available_iogrps'] = (self._assistant.
                                           get_available_io_groups())

        # Get the capability of extending a cloning volume
        self._state['cloned_extend_support'] = (
            self._assistant.extend_cloning_support(capabilities_value))

        # Get the capability of fast localcopy consistgrp
        self._state['fastconsistgrp_support'] = (
            self._assistant.fastconsisgrp_support(capabilities_value))

        # Get the capability of extending a volume which in lcmap
        self._state['lcmap_remotecopy_extend_support'] = (
            self._assistant.lcmap_remotecopy_extend_support(capabilities_value))

        # Get the iSCSI and FC names of the InStorage/MCS nodes
        self._state['storage_nodes'] = self._init_node_info()

    def _init_node_info(self):
        """get the controller node infomation."""
        nodes = {}
        nodes = self._assistant.get_node_info()
        # Add the IP addresses and WWPNs to the storage node info
        self._assistant.add_ip_addrs(nodes)
        self._assistant.add_fc_wwpns(nodes)
        # For each node, check what connection modes it supports.  Delete any
        # nodes that do not support any types (may be partially configured).
        to_delete = []
        for k, node in nodes.items():
            if ((len(node['ipv4']) or len(node['ipv6'])) and
                    len(node['iscsi_name'])):
                node['enabled_protocols'].append('iSCSI')
                self._state['enabled_protocols'].add('iSCSI')
            if len(node['WWPN']):
                node['enabled_protocols'].append('FC')
                self._state['enabled_protocols'].add('FC')
            if len(node['ipv4']) or len(node['ipv6']):
                node['enabled_protocols'].append('RDMA')
                self._state['enabled_protocols'].add('RDMA')
            if not node['enabled_protocols']:
                to_delete.append(k)
        for delkey in to_delete:
            del nodes[delkey]
        return nodes

    def _get_backend_pools(self):
        if not self._active_backend_id:
            return self.configuration.instorage_mcs_volpool_name
        elif not self._secondary_pools:
            self._secondary_pools = [self._replica_target.get('pool_name')]
        return self._secondary_pools

    def _validate_pools_exist(self):
        # Validate that the pool exists
        pools = self._get_backend_pools()
        for pool in pools:
            try:
                self._assistant.get_pool_attrs(pool)
            except exception.VolumeBackendAPIException:
                msg = _('Failed getting details for pool %s.') % pool
                raise exception.InvalidInput(reason=msg)

    def check_for_setup_error(self):
        """Ensure that the flags are set properly."""
        LOG.debug('enter: check_for_setup_error')

        # Check that we have the system ID information
        if self._state['system_name'] is None:
            exception_msg = (_('Unable to determine system name.'))
            raise exception.VolumeBackendAPIException(data=exception_msg)
        if self._state['system_id'] is None:
            exception_msg = (_('Unable to determine system id.'))
            raise exception.VolumeBackendAPIException(data=exception_msg)

        # Make sure we have at least one node configured
        if not self._state['storage_nodes']:
            msg = _('do_setup: No configured nodes.')
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        if self.protocol not in self._state['enabled_protocols']:
            raise exception.InvalidInput(
                reason=_('The storage device does not support %(prot)s. '
                         'Please configure the device to support %(prot)s or '
                         'switch to a driver using a different protocol.')
                % {'prot': self.protocol})

        required_flags = ['san_ip', 'san_ssh_port', 'san_login',
                          'instorage_mcs_volpool_name']
        for flag in required_flags:
            if not self.configuration.safe_get(flag):
                raise exception.InvalidInput(reason=_('%s is not set.') % flag)

        if not (self.configuration.san_password or
                self.configuration.san_private_key):
            raise exception.InvalidInput(
                reason=_('Password or SSH private key is required for '
                         'authentication: set either san_password or '
                         'san_private_key option.'))

        if (self.configuration.instorage_mcs_fastconsistgrp_support
                and not self._state['fastconsistgrp_support']):
            raise exception.InvalidInput(
                reason=_('the storage do not support fast localcopy consistgrp '
                        'and the instorage_mcs_fastconsistgrp_support should be '
                        'False'))


        opts = self._assistant.build_default_opts(self.configuration)
        self._assistant.check_vdisk_opts(self._state, opts)

        LOG.debug('leave: check_for_setup_error')

    def _run_ssh(self, cmd_list, check_exit_code=True, attempts=3):
        """SSH tool"""
        cinder_utils.check_ssh_injection(cmd_list)
        command = ' '.join(cmd_list)
        if not self.sshpool:
            for retry_num in range(attempts):
                temp_active_ip = self.active_ip
                try:
                    self.sshpool = self._set_up_sshpool(self.active_ip)
                    self.sshpool_available = True
                    break
                except paramiko.SSHException:
                    if retry_num == attempts - 1:
                        LOG.error('_run_ssh, Retry was exceeded. Unable '
                                  'to create SSHPool using san_ip')
                        if self._switch_ip(temp_active_ip):
                            try:
                                self.sshpool = self._set_up_sshpool(self.active_ip)
                                self.sshpool_available = True
                                break
                            except paramiko.SSHException:
                                with excutils.save_and_reraise_exception():
                                    LOG.error(_('Unable to create SSHPool using'
                                                'instorage_san_secondary_ip, '
                                                'Please check the network.'))
                        else:
                            with excutils.save_and_reraise_exception():
                                LOG.error('Unable to create SSHPool using san_ip '
                                            'and instorage_san_secondary_ip is '
                                            'not configured. '
                                            'Please check the network.')
                    LOG.warning('_run_ssh, retry, '
                                'ip: %(ip)s, '
                                'TryNum: %(rn)s.',
                                {'ip': self.active_ip,
                                 'rn': retry_num + 1})
                    greenthread.sleep(random.randint(20, 500) / 100.0)
        self.request_semaphore.acquire()
        temp_active_ip = self.active_ip
        try:
            while not self.sshpool_available:
                LOG.warning('wait for sshpool available')
                greenthread.sleep(3)
            out, err = self._ssh_execute(self.sshpool, command, check_exit_code, attempts)
            self.request_semaphore.release()
            return out, err
        except processutils.ProcessExecutionError as ex:
            self.request_semaphore.release()
            if ('mcsinq' in ex.cmd or 'mcsop' in ex.cmd) and 'CMMVC' in ex.stderr:
                with excutils.save_and_reraise_exception():
                    self.sshpool_available = True
                    LOG.error(_("Error running SSH command: %s"),
                              command)
            else:
                with excutils.save_and_reraise_exception():
                    LOG.error("command execute failed. Please check the network"
                              " and storage cluster")
        except Exception:
            # Need to check if creating an SSHPool instorage_san_secondary_ip
            # before raising an error.
            try:
                self.sshpool_available = False
                if self.sshpool:
                    del self.sshpool
                    self.sshpool = None
                if self._switch_ip(temp_active_ip):
                    self.sshpool = self._set_up_sshpool(self.active_ip)
                    self.sshpool_available = True
                    out, err = self._ssh_execute(self.sshpool, command,
                                check_exit_code, attempts)
                    self.request_semaphore.release()
                    return out, err
                else:
                    self.sshpool_available = True
                    LOG.warning(_('unable to use '
                                  'instorage_san_secondary_ip since it is '
                                  'not configured.'))
                    self.request_semaphore.release()
                    raise
            except processutils.ProcessExecutionError as ex:
                self.request_semaphore.release()
                if ('mcsinq' in ex.cmd or 'mcsop' in ex.cmd) and 'CMMVC' in ex.stderr:
                    with excutils.save_and_reraise_exception():
                        self.sshpool_available = True
                        LOG.error(_("Error running SSH command: %s"),
                                command)
            except Exception:
                with excutils.save_and_reraise_exception():
                    self.sshpool_available = False
                    self.request_semaphore.release()
                    LOG.error("command execute failed. Please check the network"
                              " and storage cluster")

    def _set_up_sshpool(self, san_ip):
        password = self.configuration.san_password
        privatekey = self.configuration.san_private_key
        if ((self.configuration.instorage_rsa_private_key is not None) and
               (self.configuration.instorage_rsa_private_key.strip() != '')):
            self.rsa_util = RsaUtil(self.configuration.instorage_rsa_private_key)
            password = self.rsa_util.decrypt_by_private_key(password)
        min_size = self.configuration.ssh_min_pool_conn
        max_size = self.configuration.ssh_max_pool_conn
        sshpool = ssh_utils.SSHPool(
            san_ip,
            self.configuration.san_ssh_port,
            self.configuration.ssh_conn_timeout,
            self.configuration.san_login,
            password=password,
            privatekey=privatekey,
            min_size=min_size,
            max_size=max_size,
            order_as_stack=True)
        return sshpool

    def _ssh_execute(self, sshpool, command,
                     check_exit_code=True, ssh_attempts=1):
        attempts = ssh_attempts
        while attempts > 0:
            attempts -= 1
            try:
                sshpool = self.sshpool
                while not self.sshpool_available:
                    greenthread.sleep(1)
                    sshpool = self.sshpool
                with sshpool.item() as ssh:
                    out, err = processutils.ssh_execute(
                        ssh,
                        command,
                        timeout=self.configuration.ssh_exec_timeout,
                        check_exit_code=check_exit_code)
                    if err and 'CMMVC' not in err:
                        LOG.error('ignore storage error: %s' % err)
                        err = ""
                    return out, err
            except processutils.ProcessExecutionError:
                with excutils.save_and_reraise_exception():
                    LOG.error(_('Error running SSH command: %s') % command)
            except paramiko.ssh_exception.SSHException:
                msg = (_('running SSH command: %s failed due to SSHException. '
                        'please check the network') % command)
                LOG.warning(msg)
                greenthread.sleep(random.randint(20, 500) / 100.0)
            except socket.timeout:
                msg = (_('running SSH command: %s timeout. '
                        'please check the network and ensure storage cluster normal ') % command)
                LOG.warning(msg)
                greenthread.sleep(random.randint(20, 500) / 100.0)
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.error('execute a ssh command failed '
                              'due to the Unknown Exception')
        msg = (_('running SSH command: %(command)s failed after %(attempts)d retries. ') %
                {'command': command,
                 'attempts':ssh_attempts})
        LOG.error(msg)
        raise exception.VolumeDriverException(reason=msg)

    def _switch_ip(self,temp_active_ip):
        # Change active_ip if instorage_san_secondary_ip is set.
        if self.configuration.instorage_san_secondary_ip is None:
            return False
        if temp_active_ip == self.active_ip:
            self.inactive_ip, self.active_ip = self.active_ip, self.inactive_ip
            LOG.info(_('Switch active_ip from %(old)s to '
                       '%(new)s.'),
                     {'old': self.inactive_ip,
                      'new': self.active_ip})
        return True

    def _run_rest(self, method, params=None,
                        request_type='get',
                        fail_handler=None):
        if not self._rest:
            self._rest = RestExecutor(self.active_ip,
                                      self.port,
                                      self.configuration.san_login,
                                      self.configuration.san_password,
                                      ca_root=self.ca_root,
                                      ca_key=self.ca_key,
                                      ca_cert=self.ca_cert,
                                      use_ssl=self.use_ssl,
                                      check_root_cert=self.check_root_cert,
                                      check_client_cert=self.check_client_cert,
                                      host_identifier=self.host_identifier,
                                      thread_num=self.configuration.instorage_mcs_thread_num,
                                      cluster_id = self.cluster_id)

        return self._rest_execute(self._rest, method, params, request_type, fail_handler)

    def _rest_execute(self, rest_client, method,
                     params=None, request_type='get',
                     fail_handler=None):
        return rest_client.send_rest_api(method, params, request_type, fail_handler)

    def generate_volume_name(self, volume):
        if not hasattr(volume,'_name_id'):
            return "snapshot-%s" % volume['id']
        else:
            return "volume-%s" % (volume['id'] if not volume['_name_id'] else volume['_name_id'])

    def generate_snapshot_name(self, snapshot):
        return "snapshot-%s" % snapshot['id']

    def generate_rccg_name(self, group):
        return  ("rccg-%s" % group.id)[0:15]

    def ensure_export(self, ctxt, volume):
        """Check that the volume exists on the storage."""
        vol_name = self._get_target_vol(volume)
        volume_defined = self._assistant.is_vdisk_defined(vol_name)

        if not volume_defined:
            LOG.error(_('ensure_export: Volume %s not found on storage.'),
                      vol_name)

    def _get_vdisk_params(self, type_id, volume_type=None,
                          volume_metadata=None):
        return self._assistant.get_vdisk_params(
            self.configuration,
            self._state,
            type_id,
            volume_type=volume_type,
            volume_metadata=volume_metadata)

    def create_volume(self, volume):
        vol_name = self.generate_volume_name(volume)
        LOG.debug('enter: create_volume: volume %s', vol_name)
        opts = self._get_vdisk_params(
            volume['volume_type_id'],
            volume_metadata=volume.get('volume_metadata'))
        pool = volume_utils.extract_host(volume['host'], 'pool')

        opts['iogrp'] = self._assistant.select_io_group(self._state, opts)

        if opts['aa']:
            self._assistant.create_volume(
                vol_name, str(volume['size']), 'gb', pool, opts
            )
        else:
            self._assistant.create_vdisk(
                vol_name, str(volume['size']), 'gb', pool, opts,
                self.configuration.instorage_mcs_vol_nofmtdisk
            )

        if opts['qos']:
            self._assistant.add_vdisk_qos(vol_name, opts['qos'])

        ctxt = context.get_admin_context()
        rep_type = self._get_volume_replicated_type(ctxt, volume)

        if rep_type:
            group_name = None
            if volume.group_id:
                if not volume_utils.is_group_a_type(
                        volume.group, 'consistent_group_replication_enabled'):
                    msg = _('Create volume with a replication '
                    'group_id is not supported. Please add volume to '
                    'group after volume creation.')
                    LOG.error(msg)
                    raise exception.VolumeDriverException(reason=msg)
                group_name = self.generate_rccg_name(volume.group)

            replica_obj = self._get_replica_obj(rep_type)
            replica_obj.volume_replication_setup(ctxt, volume, group_name)
            model_update = {'replication_status': 'enabled'}
        else:
            model_update = {'replication_status': 'disabled'}
        if self.icos_support:
            model_update['is_active_active'] = opts['aa']
            model_update['provision_type'] = 'thick' if opts['rsize'] in [-1,100] else 'thin'
        LOG.debug('leave: create_volume:\n volume: %(vol)s\n '
                  'model_update %(model_update)s',
                  {'vol': vol_name,
                   'model_update': model_update})
        return model_update

    def create_volume_from_snapshot(self, volume, snapshot, link_clone=False):
        vol_name = self.generate_volume_name(volume)
        snap_name = self.generate_snapshot_name(snapshot)

        if snapshot['volume_size'] > volume['size']:
            msg = (_("create_volume_from_snapshot: snapshot %(snapshot_name)s "
                     "size is %(snapshot_size)dGB and doesn't fit in target "
                     "volume %(volume_name)s of size %(volume_size)dGB.") %
                   {'snapshot_name': snap_name,
                    'snapshot_size': snapshot['volume_size'],
                    'volume_name': vol_name,
                    'volume_size': volume['size']})
            LOG.error(msg)
            raise exception.InvalidInput(message=msg)

        opts = self._get_vdisk_params(
            volume['volume_type_id'],
            volume_metadata=volume.get('volume_metadata'))
        pool = volume_utils.extract_host(volume['host'], 'pool')
        full_copy = False if link_clone else True
        self._assistant.create_copy(snap_name, vol_name,
                                    snapshot['id'], self.configuration,
                                    opts, full_copy, pool=pool, nofmtdisk=True)

        if volume['size'] > snapshot['volume_size']:
            # extend the new created target volume to expected size.
            self._extend_volume_op(volume, volume['size'], snapshot['volume_size'])

        if opts['qos']:
            self._assistant.add_vdisk_qos(vol_name, opts['qos'])

        if opts['aa']:
            # what we need is an active-active volume
            # we need wait clone finish and then
            # transfter that vdisk to a volume
            self._assistant.ensure_vdisk_no_lc_mappings(vol_name, allow_snaps=False)
            self._assistant.transfer_vdisk_to_volume(vol_name, pool, opts)

        ctxt = context.get_admin_context()
        rep_type = self._get_volume_replicated_type(ctxt, volume)

        model_update = dict()
        if rep_type:
            self._validate_replication_enabled()
            replica_obj = self._get_replica_obj(rep_type)
            replica_obj.volume_replication_setup(ctxt, volume)
            model_update['replication_status'] = 'enabled'
        else:
            model_update = {'replication_status': 'disabled'}
        if self.icos_support:
            model_update['is_active_active'] = opts['aa']
            model_update['provision_type'] = 'thick' if opts['rsize'] in [-1,100] else 'thin'
        return model_update

    def create_cloned_volume(self, tgt_volume, src_volume):
        """Creates a clone of the specified volume."""
        src_vol_name = self.generate_volume_name(src_volume)
        tgt_vol_name = self.generate_volume_name(tgt_volume)

        if src_volume['size'] > tgt_volume['size']:
            msg = (_("create_cloned_volume: source volume %(src_vol)s "
                     "size is %(src_size)dGB and doesn't fit in target "
                     "volume %(tgt_vol)s of size %(tgt_size)dGB.") %
                   {'src_vol': src_vol_name,
                    'src_size': src_volume['size'],
                    'tgt_vol': tgt_vol_name,
                    'tgt_size': tgt_volume['size']})
            LOG.error(msg)
            raise exception.InvalidInput(message=msg)

        opts = self._get_vdisk_params(
            tgt_volume['volume_type_id'],
            volume_metadata=tgt_volume.get('volume_metadata'))
        pool = volume_utils.extract_host(tgt_volume['host'], 'pool')
        self._assistant.create_copy(src_vol_name, tgt_vol_name,
                                    src_volume['id'], self.configuration,
                                    opts, True, pool=pool, nofmtdisk=True)

        # The source volume size is equal to target volume size
        # in most of the cases. But in some scenarios, the target
        # volume size may be bigger than the source volume size.
        # InStorage does not support localcopy between two volumes
        # with two different sizes. So InStorage will copy volume
        # from source volume first and then extend target
        # volume to original size.
        if tgt_volume['size'] > src_volume['size']:
            # extend the new created target volume to expected size.
            self._extend_volume_op(tgt_volume, tgt_volume['size'], src_volume['size'])

        if opts['qos']:
            self._assistant.add_vdisk_qos(tgt_vol_name, opts['qos'])

        if opts['aa']:
            # what we need is an active-active volume
            # we need wait clone finish and then
            # transfter that vdisk to a volume
            self._assistant.ensure_vdisk_no_lc_mappings(tgt_vol_name, allow_snaps=False)
            self._assistant.transfer_vdisk_to_volume(tgt_vol_name, pool, opts)

        ctxt = context.get_admin_context()
        rep_type = self._get_volume_replicated_type(ctxt, tgt_volume)

        model_update = dict()
        if rep_type:
            self._validate_replication_enabled()
            replica_obj = self._get_replica_obj(rep_type)
            replica_obj.volume_replication_setup(ctxt, tgt_volume)
            model_update['replication_status'] = 'enabled'
        else:
            model_update = {'replication_status': 'disabled'}
        if self.icos_support:
            model_update['is_active_active'] = opts['aa']
            model_update['provision_type'] = 'thick' if opts['rsize'] in [-1,100] else 'thin'
        return model_update

    def extend_volume(self, volume, new_size):
        volume_name = self._get_target_vol(volume)
        is_remote_vdisk = False
        is_aa_vdisk = self._assistant.is_aa_vdisk(volume_name)
        rel_info = self._assistant.get_relationship_info(volume_name)
        if not is_aa_vdisk:
            if rel_info:
                is_remote_vdisk = True

        ret = self._assistant.ensure_vdisk_not_formatting(volume_name)
        if not ret:
            msg = (_('_extend_volume_op: Extending a formatting volume is '
                     'not supported.'))
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        #check the volume is the source in lcmap or not
        if not self._state['lcmap_remotecopy_extend_support']:
            src_mappings, __ = self._assistant.get_vdisk_lc_mappings(
                                                volume_name, type='source')
            if is_aa_vdisk:
                if len(src_mappings) >= 2:
                    msg = (_('extend_volume: Extending an Active-Active volume'
                            ' with snapshots is not supported.'))
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)
            else:
                if len(src_mappings) >= 1:
                    msg = (_('extend_volume: Extending a volume with snapshots'
                            ' is not supported.'))
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)

        self._extend_volume_op(volume, new_size, is_aa_vdisk=is_aa_vdisk,
            is_remote_vdisk=is_remote_vdisk, rel_info=rel_info)

    def _extend_volume_op(self, volume, new_size, old_size=None, is_aa_vdisk=False,
                          is_remote_vdisk=False, rel_info=None):
        LOG.debug('enter: _extend_volume_op: volume %s', volume['id'])
        volume_name = self._get_target_vol(volume)
        if old_size is None:
            old_size = volume['size']
        extend_amt = int(new_size) - old_size

        if is_aa_vdisk:
            LOG.info("extend Active-Active volume begin.")
            if not self._state['lcmap_remotecopy_extend_support']:
                opts = self._get_vdisk_params(
                    volume['volume_type_id'],
                    volume_metadata=volume.get('volume_metadata'))
                pool = volume_utils.extract_host(volume['host'], 'pool')
                # first transfer it to a general vdisk then do extend
                self._assistant.transfer_volume_to_vdisk(volume_name, pool)
                self._assistant.extend_vdisk(volume_name, extend_amt)
                self._assistant.transfer_vdisk_to_volume(volume_name, pool, opts)
            else:
                if rel_info:
                    self._assistant.extend_rc_relationship(extend_amt, "all", rel_info['id'])

        elif is_remote_vdisk:
            LOG.info('extend remotecopy volume begin.')
            if not self._state['lcmap_remotecopy_extend_support']:
                try:
                    vol_name = self.generate_volume_name(volume)

                    tgt_vol = instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name
                    rep_type = rel_info['copy_type']
                    self._local_backend_assistant.delete_relationship(vol_name)
                    self._local_backend_assistant.extend_vdisk(vol_name,
                                                            extend_amt)
                    self._aux_backend_assistant.extend_vdisk(tgt_vol, extend_amt)
                    tgt_sys = self._aux_backend_assistant.get_system_info()
                    self._local_backend_assistant.create_relationship(
                        vol_name, tgt_vol, tgt_sys.get('system_name'),
                        True if instorage_const.ASYNC == rep_type else False)
                except Exception as exp:
                    msg = (_('Failed to extend a volume with remote copy '
                            '%(volume)s. Exception: '
                            '%(err)s.') % {'volume': volume['id'],
                                            'err': six.text_type(exp)})
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)
            else:
                vol_name = self.generate_volume_name(volume)
                tgt_vol = instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name
                master_vol_attr = self._local_backend_assistant.get_vdisk_attributes(vol_name)
                aux_vol_attr = self._aux_backend_assistant.get_vdisk_attributes(tgt_vol)
                if master_vol_attr and aux_vol_attr:
                    ret = self._aux_backend_assistant.ensure_vdisk_not_formatting(tgt_vol)
                    if not ret:
                        msg = (_('_extend_volume_op: Extending a formatting volume is '
                                'not supported.'))
                        LOG.error(msg)
                        raise exception.VolumeDriverException(message=msg)
                    master_rc_id = master_vol_attr["RC_id"]
                    aux_rc_id = aux_vol_attr["RC_id"]
                    master_vol_size = int(math.ceil(float(master_vol_attr['capacity']) / units.Gi))
                    aux_vol_size = int(math.ceil(float(aux_vol_attr['capacity']) / units.Gi))
                    master_extend_amt = new_size - master_vol_size
                    aux_extend_amt = new_size - aux_vol_size
                    if master_extend_amt < 0:
                        msg = (_('Failed to extend a volume with remote copy '
                            '%(volume)s. because the master vdisk size '
                            'is greater than the target size') % {'volume': volume['id']})
                        LOG.error(msg)
                        raise exception.VolumeDriverException(message=msg)
                    if aux_extend_amt < 0:
                        msg = (_('Failed to extend a volume with remote copy '
                            '%(volume)s. because the aux vdisk size '
                            'is greater than the target size') % {'volume': volume['id']})
                        LOG.error(msg)
                        raise exception.VolumeDriverException(message=msg)
                    if master_extend_amt > 0:
                        self._local_backend_assistant.extend_rc_relationship(
                            master_extend_amt, "master", master_rc_id)
                    if aux_extend_amt > 0:
                        self._aux_backend_assistant.extend_rc_relationship(
                            aux_extend_amt, "aux", aux_rc_id)
                    self._local_backend_assistant.start_relationship(volume_name)

                else:
                    msg = (_('Failed to extend a volume with remote copy '
                            '%(volume)s.') % {'volume': volume['id']})
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)

        else:
            LOG.info('extend common/thin/compressed/dedup volume begin.')
            self._assistant.extend_vdisk(volume_name, extend_amt)

        LOG.debug('leave: _extend_volume_op: volume %s', volume['id'])

    def delete_volume(self, volume):
        vol_name = self.generate_volume_name(volume)

        LOG.debug('enter: delete_volume: volume %s', vol_name)
        ctxt = context.get_admin_context()

        rep_type = self._get_volume_replicated_type(ctxt, volume)
        if rep_type:
            self._aux_backend_assistant.delete_rc_volume(vol_name,
                                                         target_vol=True)
            if not self._active_backend_id:
                self._local_backend_assistant.delete_rc_volume(vol_name)
            else:
                # If it's in fail over state, also try to delete the volume
                # in master backend
                try:
                    self._local_backend_assistant.delete_rc_volume(vol_name)
                except Exception as ex:
                    LOG.error(_('Failed to get delete volume %(volume)s in '
                                'master backend. Exception: %(err)s.'),
                              {'volume': vol_name,
                               'err': six.text_type(ex)})
        else:
            if self._active_backend_id:
                msg = (_('Error: delete non-replicate volume in failover mode'
                         ' is not allowed.'))
                LOG.error(msg)
                raise exception.VolumeDriverException(message=msg)
            else:
                self._assistant.delete_vdisk(vol_name, False)

        if volume['id'] in self._vdiskcopyops:
            del self._vdiskcopyops[volume['id']]

            if not self._vdiskcopyops:
                self._vdiskcopyops_loop.stop()
                self._vdiskcopyops_loop = None
        LOG.debug('leave: delete_volume: volume %s', vol_name)

    def format_volume(self, volume, fstype='ext4'):
        uuid = None
        attach_info = self._attach_device(volume)
        backup_device_path = attach_info['device']['path']
        try:
            # format device
            root_helper = cinder_utils.get_root_helper()
            if fstype in ['ext2','ext3','ext4']:
                cmd = ['mkfs', '-t', fstype, '-F','-m0',backup_device_path]
            elif fstype in ['xfs']:
                cmd = ['mkfs', '-t', fstype, '-f',backup_device_path]
            processutils.execute(*cmd, root_helper=root_helper, run_as_root=True)
            # get uuid of partition
            cmd = ['blkid', backup_device_path]
            stdout, _ = processutils.execute(*cmd, root_helper=root_helper, run_as_root=True)
            if 'PTUUID' in stdout:
                uuid_str = stdout.split(' ')[1]
                uuid = uuid_str[8:len(uuid_str)-1]
            elif 'UUID' in stdout:
                uuid_str = stdout.split(' ')[1]
                uuid = uuid_str[6:len(uuid_str)-1]
        except Exception as err:
            with excutils.save_and_reraise_exception():
                LOG.error(
                    'Format device %s failed for %s.',
                    volume['id'],
                    err
                )
        self._detach_device(volume, attach_info)
        return uuid

    def _attach_device(self, volume):
        properties = cinder_utils.brick_get_connector_properties()
        conn = self.initialize_connection(volume, properties)
        connector = cinder_utils.brick_get_connector(
            conn['driver_volume_type'],
            use_multipath=properties.get('multipath', False),
            device_scan_attempts=5,
            conn=conn,
            expect_raw_disk=True
        )
        vol_handle = connector.connect_volume(conn['data'])

        attach_info = {
            'conn': conn,
            'device': vol_handle,
            'connector': connector,
            'properties': properties
        }
        return attach_info

    def _detach_device(self, volume, attach_info):
        connector = attach_info['connector']
        connector.disconnect_volume(
            attach_info['conn']['data'],
            attach_info['device'],
        )
        self.terminate_connection(volume, attach_info['properties'])

    @utils_trace
    @log_filter.register_function()
    def create_snapshot(self, snapshot):
        ctxt = context.get_admin_context()
        try:
            source_vol = self.db.volume_get(ctxt, snapshot['volume_id'])
        except Exception:
            msg = (_('create_snapshot: get source volume failed.'))
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)
        pool = volume_utils.extract_host(source_vol['host'], 'pool')
        opts = self._get_vdisk_params(source_vol['volume_type_id'])
        snap_name = self.generate_snapshot_name(snapshot)
        snap_vol_name = self.generate_volume_name(snapshot['volume'])
        if opts['aa']:
            rep_info = self._assistant.get_relationship_info(snap_vol_name)
            if rep_info:
                if rep_info['primary'] == 'master':
                    if 'master_vdisk_name' in rep_info:
                        snap_vol_name = rep_info['master_vdisk_name']
                    else:
                        snap_vol_name = rep_info['masterVdiskName']
                elif rep_info['primary'] == 'aux':
                    if 'aux_vdisk_name' in rep_info:
                        snap_vol_name = rep_info['aux_vdisk_name']
                    else:
                        snap_vol_name = rep_info['auxVdiskName']
            else:
                msg = ('failed to create a snapshot for A-A volume due to missing rcrelationship')
                LOG.error(msg)
                raise exception.VolumeDriverException(message=msg)

        opts['rsize'] = 0
        self._assistant.create_copy(snap_vol_name, snap_name,
                                    snapshot['volume_id'], self.configuration,
                                    opts, False, pool=pool)

    def delete_snapshot(self, snapshot):
        snapshot_info = self.get_snapshot_info(snapshot)
        LOG.debug("snapshot_info")
        LOG.debug(snapshot_info)
        snap_name = self.generate_snapshot_name(snapshot)
        self._assistant.delete_vdisk(snap_name, False)

    def rollback_snapshot(self, snapshot, volume):
        """rollback_snapshot is a tiexinCloud specific interface."""
        ctxt = context.get_admin_context()
        self.revert_to_snapshot(ctxt,volume,snapshot)

    def rollback_snapshot(self, context, snapshot, volume=None):
        """rollback_snapshot is a Huawei FusionSphere specific interface."""
        src_volume = volume
        if not src_volume:
            src_volume = snapshot['volume']
        self.revert_to_snapshot(context, src_volume, snapshot)

    def revert_to_snapshot(self, context, volume, snapshot):
        if snapshot['volume_size'] != volume['size']:
            msg = ('Revert volume size must equal with the snapshot size')
            raise exception.InvalidInput(reason=msg)

        rep_type = self._get_volume_replicated_type(context, volume)
        vol_name = self.generate_volume_name(volume)
        snap_name = self.generate_snapshot_name(snapshot)
        is_aa_vdisk = self._assistant.is_aa_vdisk(vol_name)
        real_vol_name = vol_name

        primary = None
        if is_aa_vdisk:
            rep_info = self._assistant.get_relationship_info(vol_name)
            if rep_info:
                if rep_info['state'] not in['consistent_synchronized',
                                            'consistent_stopped', 'idling']:
                    msg = (('the state of the rcrelationship of %(vol_name)s is %(state)s') %
                           {'vol_name': vol_name,
                            'state': rep_info['state']})
                    raise exception.VolumeDriverException(message=msg)
                _, tgt_mappings = self._assistant.get_vdisk_lc_mappings(
                                                  snap_name, type='target')
                if len(tgt_mappings) > 1:
                    msg = ('unexpect error: snapshot volume in more than one lcmap')
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)
                lcmap = tgt_mappings[0]
                snap_vol_name = (lcmap['source_vdisk_name'] if 'source_vdisk_name' in lcmap
                                                            else lcmap['sourceVdiskName'])
                real_vol_name = snap_vol_name
                if snap_vol_name.startswith('volume-'):
                    primary = 'master'
                else:
                    primary = 'aux'
                self._assistant.stop_relationship(vol_name, True)
            else:
                msg = ('failed to restore a snapshot for A-A volume'
                       'due to missing rcrelationship')
                LOG.error(msg)
                raise exception.VolumeDriverException(message=msg)
        else:
            if rep_type:
                self._assistant.stop_relationship(vol_name)
            src_mappings, tgt_mappings = self._assistant.get_vdisk_lc_mappings(
                                                        vol_name, type='target')
            for mappings in [src_mappings, tgt_mappings]:
                for attrs in mappings:
                    map_id = attrs['id']
                    status = attrs['status']
                    progress = attrs['progress']
                    source = (attrs['source_vdisk_name'] if 'source_vdisk_name' in attrs
                                                         else attrs['sourceVdiskName'])
                    target = (attrs['target_vdisk_name'] if 'target_vdisk_name' in attrs
                                                         else attrs['targetVdiskName'])
                    if source == snap_name:
                        continue
                    if progress not in [100,'100']:
                        msg = ('the source volume of the snapshot is a target'
                                'in a clone rcrelationship'
                                'and the clone progress is not reach 100%,'
                                'please try again after a few minites')
                        raise exception.VolumeDriverException(message=msg)
                    elif (status == 'copying' and progress in [100,'100']):
                        # stop and delete lcmap
                        temp_src_mappings, _ = self._assistant.get_vdisk_lc_mappings(
                                                   target, type='source')
                        if temp_src_mappings:
                            self._assistant.stop_lcmap(map_id, split=True)
                        else:
                            self._assistant.stop_lcmap(map_id, split=False)

                        attrs = self._assistant.get_localcopy_mapping_attributes(map_id)
                        if attrs:
                            if attrs['status'] == 'idle_or_copied':
                                try:
                                    self._assistant.rm_lcmap(map_id)
                                except exception.VolumeBackendAPIException as ex:
                                    if 'CMMVC6318E' not in ex.msg:
                                        raise exception.VolumeDriverException(
                                            message=ex.msg)
                            else:
                                msg = ('the clean progress is not reach 100%,'
                                    'please try again after a few minites')
                                raise exception.VolumeDriverException(message=msg)
                    elif status =='idle_or_copied':
                        try:
                            self._assistant.rm_lcmap(map_id)
                        except exception.VolumeBackendAPIException as ex:
                            if 'CMMVC6318E' not in ex.msg:
                                raise exception.VolumeDriverException(
                                    message=ex.msg)

        try:
            self._assistant.run_localcopy(
                snap_name,
                real_vol_name,
                self.configuration.instorage_mcs_localcopy_timeout,
                self.configuration.instorage_mcs_localcopy_rate,
                self.configuration.instorage_mcs_localcopy_clean_rate,
                full_copy=True,
                restore=True
            )
            self._assistant.ensure_rollback_snapshot_ok(real_vol_name, snap_name)
            if is_aa_vdisk:
                self._assistant.start_relationship(vol_name, primary)
            elif rep_type:
                self._assistant.start_relationship(vol_name)
        except Exception as err:
            with excutils.save_and_reraise_exception():
                LOG.error(
                    'Revert to snapshot from %s to %s failed for %s.',
                    snap_name,
                    vol_name,
                    err
                )

    def get_snapshot_info(self, snapshot):
        """private interface for HK cloud for retriveve the capcacity of a snapshot"""
        snap_name = self.generate_snapshot_name(snapshot)
        result = {
            'name': snap_name,
            'capacity': 0,
            'used_capacity': 0
        }
        vdisk_copy = self._assistant.get_vdisk_copy_attrs(snap_name, '0')
        if vdisk_copy:
            result.update({'capacity': vdisk_copy['capacity']})
            result.update({'used_capacity': vdisk_copy['used_capacity']})
        return result

    def get_volume_migration_progress(self, volume, src_pool=None, dest_pool=None):
        """retrieve the vdiskcopy sync progress for a volume migration"""
        progress = 0
        vol_name = self.generate_volume_name(volume)
        copies = self._assistant.get_vdisk_copies(vol_name)
        if not copies['secondary'] or not copies['primary']:
            progress = '-1'
            # if volume has been migrated successfully or
            # has not passed the driver migration,
            # the progress return to -1
        else:
            progress1 = int(copies['primary']['sync_progress'])
            progress2 = int(copies['secondary']['sync_progress'])
            progress = progress1 if progress1 <= progress2 else progress2
            progress = float(progress)
        return progress

    def add_vdisk_copy(self, volume, dest_pool, vol_type):
        return self._assistant.add_vdisk_copy(volume, dest_pool,
                                              vol_type, self._state,
                                              self.configuration)

    def _add_vdisk_copy_op(self, ctxt, volume, new_op):
        if volume['id'] in self._vdiskcopyops:
            self._vdiskcopyops[volume['id']].append(new_op)
        else:
            self._vdiskcopyops[volume['id']] = [new_op]

        # We added the first copy operation, so start the looping call
        if len(self._vdiskcopyops) == 1:
            self._vdiskcopyops_loop = loopingcall.FixedIntervalLoopingCall(
                self._check_volume_copy_ops)
            self._vdiskcopyops_loop.start(interval=self.VDISKCOPYOPS_INTERVAL)

    def _rm_vdisk_copy_op(self, ctxt, volume, orig_copy_id, new_copy_id):
        try:
            self._vdiskcopyops[volume['id']].remove((orig_copy_id,
                                                     new_copy_id))
            if not self._vdiskcopyops[volume['id']]:
                del self._vdiskcopyops[volume['id']]
            if not self._vdiskcopyops:
                self._vdiskcopyops_loop.stop()
                self._vdiskcopyops_loop = None
        except KeyError:
            LOG.error(_('_rm_vdisk_copy_op: Volume %s does not have any '
                        'registered vdisk copy operations.'), volume['id'])
            return
        except ValueError:
            LOG.error(_('_rm_vdisk_copy_op: Volume %(vol)s does not have '
                        'the specified vdisk copy operation: orig=%(orig)s '
                        'new=%(new)s.'),
                      {'vol': volume['id'], 'orig': orig_copy_id,
                       'new': new_copy_id})
            return

    def _check_volume_copy_ops(self):
        LOG.debug("Enter: update volume copy status.")
        ctxt = context.get_admin_context()
        copy_items = list(self._vdiskcopyops.items())
        for vol_id, copy_ops in copy_items:
            try:
                volume = self.db.volume_get(ctxt, vol_id)
            except Exception:
                LOG.warning(_('Volume %s does not exist.'), vol_id)
                del self._vdiskcopyops[vol_id]
                if not self._vdiskcopyops:
                    self._vdiskcopyops_loop.stop()
                    self._vdiskcopyops_loop = None
                continue

            vol_name = self.generate_volume_name(volume)
            for copy_op in copy_ops:
                try:
                    synced = self._assistant.check_vdisk_copy_synced(
                        vol_name, copy_op[1])
                except Exception:
                    LOG.info(_('_check_volume_copy_ops: Volume %(vol)s does '
                               'not have the specified vdisk copy '
                               'operation: orig=%(orig)s new=%(new)s.'),
                             {'vol': volume['id'], 'orig': copy_op[0],
                              'new': copy_op[1]})
                else:
                    if synced:
                        self._assistant.rm_vdisk_copy(vol_name, copy_op[0])
                        self._rm_vdisk_copy_op(ctxt, volume, copy_op[0],
                                               copy_op[1])
        LOG.debug("Exit: update volume copy status.")

    def migrate_volume(self, ctxt, volume, host):
        """Migrate directly if source and dest are managed by same storage.

        We create a new vdisk copy in the desired pool, and add the original
        vdisk copy to the admin_metadata of the volume to be deleted. The
        deletion will occur using a periodic task once the new copy is synced.

        :param ctxt: Context
        :param volume: A dictionary describing the volume to migrate
        :param host: A dictionary describing the host to migrate to, where
                     host['host'] is its name, and host['capabilities'] is a
                     dictionary of its reported capabilities.
        """
        LOG.debug('enter: migrate_volume: id=%(id)s, host=%(host)s',
                  {'id': volume['id'], 'host': host['host']})
        false_ret = (False, None)
        volume_name = self.generate_volume_name(volume)
        # migrate a InMetro volume must be through the host side
        if self._assistant.is_aa_vdisk(volume_name):
            msg = _('migrate: migrate a InMetro volume must be through the host side')
            LOG.warning(msg)
            return false_ret

        dest_pool = self._assistant.can_migrate_to_host(host, self._state)
        if dest_pool is None:
            return false_ret

        ctxt = context.get_admin_context()
        volume_type_id = volume['volume_type_id']
        if volume_type_id is not None:
            vol_type = volume_types.get_volume_type(ctxt, volume_type_id)
        else:
            vol_type = None

        # migrate volume with vdisk copy and without vdisk copy
        resp = self._assistant.executor.lsvdiskcopy(volume_name)
        if len(resp) > 1:
            copies = self._assistant.get_vdisk_copies(volume_name)
            src_pool = copies['primary']['mdisk_grp_name']
            if self._assistant.is_ssa_pool(src_pool):
                msg = _('Unable to migrate: the SSA '
                        'volume with volume copy can not be migrated. ')
                raise exception.VolumeDriverException(message=msg)
            self._assistant.migratevdisk(volume_name, dest_pool,
                                         copies['primary']['copy_id'])
        else:
            self._check_volume_copy_ops()
            new_op = self.add_vdisk_copy(volume_name, dest_pool, vol_type)
            self._add_vdisk_copy_op(ctxt, volume, new_op)

        if CONF.migration_waiting_for_copy:
            copy_status = self._vdiskcopyops
            LOG.info("Volume %s data is being copied in storage", volume['id'])
            while volume.id in copy_status:
                copy_status = self._vdiskcopyops
                time.sleep(1)
            LOG.info("Volume %s data copy completed", volume['id'])

        LOG.debug('leave: migrate_volume: id=%(id)s, host=%(host)s',
                  {'id': volume['id'], 'host': host['host']})
        return (True, None)

    def retype(self, ctxt, volume, new_type, diff, host):
        """Convert the volume to be of the new type.

        Returns a boolean indicating whether the retype occurred.

        :param ctxt: Context
        :param volume: A dictionary describing the volume to migrate
        :param new_type: A dictionary describing the volume type to convert to
        :param diff: A dictionary with the difference between the two types
        :param host: A dictionary describing the host to migrate to, where
                     host['host'] is its name, and host['capabilities'] is a
                     dictionary of its reported capabilities.
        """
        def retype_iogrp_property(volume, new, old):
            if new != old:
                vol_name = self.generate_volume_name(volume)
                self._assistant.change_vdisk_iogrp(vol_name,
                                                   self._state, (new, old))

        LOG.debug('enter: retype: id=%(id)s, new_type=%(new_type)s,'
                  'diff=%(diff)s, host=%(host)s', {'id': volume['id'],
                                                   'new_type': new_type,
                                                   'diff': diff,
                                                   'host': host})
        volume_name = self.generate_volume_name(volume)
        # InMetro volume does not support migrate
        if self._assistant.is_aa_vdisk(volume_name):
            msg = _('retype: retyping a InMetro volume must be through the host side')
            LOG.warning(msg)
            return False

        need_copy = False
        # check whether the dest_pool is on the same storage with this driver
        dest_pool = self._assistant.can_migrate_to_host(host, self._state)
        if dest_pool is None:
            return False

        # check whether pool changes
        src_pool = volume_utils.extract_host(volume['host'], 'pool')
        dest_pool = volume_utils.extract_host(host['host'], 'pool')
        if src_pool != dest_pool:
            need_copy = True

        old_opts = self._get_vdisk_params(
            volume['volume_type_id'],
            volume_metadata=volume.get('volume_matadata')
        )
        new_opts = self._get_vdisk_params(
            new_type['id'], volume_type=new_type
        )

        # cache change attribute when no need disk copy
        vdisk_changes = []
        no_copy_keys = []
        copy_keys = []
        if not need_copy:
            is_ssa_pool = self._assistant.is_ssa_pool(dest_pool)
            if is_ssa_pool:
                no_copy_keys = ['warning', 'autoexpand','deduped', 'compressed']
                copy_keys = []
            else:
                no_copy_keys = ['warning', 'autoexpand', 'intier']
                copy_keys = ['rsize', 'grainsize', 'compressed']

            all_keys = no_copy_keys + copy_keys

            for key in all_keys:
                if old_opts[key] != new_opts[key]:
                    if key in copy_keys:
                        need_copy = True
                        break
                    elif key in no_copy_keys:
                        vdisk_changes.append(key)
        vol_name = self.generate_volume_name(volume)
        # Check if retype affects volume replication
        model_update = None
        new_rep_type = self._get_specs_replicated_type(new_type)
        old_rep_type = self._get_volume_replicated_type(ctxt, volume)
        old_io_grp = self._assistant.get_volume_io_group(vol_name)

        # There are three options for rep_type: None, sync, async
        if new_rep_type != old_rep_type:
            if (old_io_grp not in
                    instorage_cli.InStorageAssistant.get_valid_requested_io_groups(
                        self._state, new_opts)):
                msg = (_('Unable to retype: it is not allowed to change '
                         'replication type and io group at the same time.'))
                LOG.error(msg)
                raise exception.VolumeDriverException(message=msg)
            if new_rep_type and old_rep_type:
                msg = (_('Unable to retype: it is not allowed to change '
                         '%(old_rep_type)s volume to %(new_rep_type)s '
                         'volume.') %
                       {'old_rep_type': old_rep_type,
                        'new_rep_type': new_rep_type})
                LOG.error(msg)
                raise exception.VolumeDriverException(message=msg)
            # If volume is replicated, can't copy
            if need_copy:
                msg = (_('Unable to retype: Current action needs volume-copy,'
                         ' it is not allowed when new type is replication.'
                         ' Volume = %s') % volume['id'])
                raise exception.VolumeDriverException(message=msg)

        new_io_grp = self._assistant.select_io_group(self._state, new_opts)

        if need_copy:
            self._check_volume_copy_ops()
            retype_iogrp_property(volume, new_io_grp, old_io_grp)

            try:
                new_op = self.add_vdisk_copy(vol_name,
                                             dest_pool,
                                             new_type)
                self._add_vdisk_copy_op(ctxt, volume, new_op)
                if CONF.migration_waiting_for_copy:
                    copy_status = self._vdiskcopyops
                    LOG.info("Volume %s data is being copied in storage", volume['id'])
                    while volume.id in copy_status:
                        copy_status = self._vdiskcopyops
                        time.sleep(1)
                    LOG.info("Volume %s data copy completed", volume['id'])
            except exception.VolumeDriverException:
                # roll back changing iogrp property
                retype_iogrp_property(volume, old_io_grp, new_io_grp)
                msg = (_('Unable to retype:  A copy of volume %s exists. '
                         'Retyping would exceed the limit of 2 copies.'),
                       volume['id'])
                raise exception.VolumeDriverException(message=msg)
        else:
            retype_iogrp_property(volume, new_io_grp, old_io_grp)

            self._assistant.change_vdisk_options(
                vol_name, vdisk_changes, new_opts
            )

        if new_opts['qos']:
            # Add the new QoS setting to the volume. If the volume has an
            # old QoS setting, it will be overwritten.
            self._assistant.update_vdisk_qos(vol_name, new_opts['qos'])
        elif old_opts['qos']:
            # If the old_opts contain QoS keys, disable them.
            self._assistant.disable_vdisk_qos(vol_name, old_opts['qos'])

        # Delete replica if needed
        if old_rep_type and not new_rep_type:
            self._aux_backend_assistant.delete_rc_volume(vol_name,
                                                         target_vol=True)
            model_update = {'replication_status': 'disabled',
                            'replication_driver_data': None,
                            'replication_extended_status': None}
        # Add replica if needed
        if not old_rep_type and new_rep_type:
            replica_obj = self._get_replica_obj(new_rep_type)
            replica_obj.volume_replication_setup(ctxt, volume)
            model_update = {'replication_status': 'enabled'}

        if new_opts.get('rsize') != old_opts.get('rsize') and self.icos_support:
            if model_update:
                model_update['provision_type'] = 'thick' if new_opts['rsize'] in [-1, 100] else 'thin'
            else:
                model_update = {'provision_type': 'thick' if new_opts['rsize'] in [-1, 100] else 'thin'}

        LOG.debug('exit: retype: ild=%(id)s, new_type=%(new_type)s,'
                  'diff=%(diff)s, host=%(host)s', {'id': volume['id'],
                                                   'new_type': new_type,
                                                   'diff': diff,
                                                   'host': host['host']})
        return True, model_update

    def update_migrated_volume(self, ctxt, volume, new_volume,
                               original_volume_status):
        """Return model update from InStorage for migrated volume.

        This method should rename the back-end volume name(id) on the
        destination host back to its original name(id) on the source host.

        :param ctxt: The context used to run the method update_migrated_volume
        :param volume: The original volume that was migrated to this backend
        :param new_volume: The migration volume object that was created on
                           this backend as part of the migration process
        :param original_volume_status: The status of the original volume
        :returns: model_update to update DB with any needed changes
        """
        name_id = None
        provider_location = None
        if original_volume_status == 'in-use':
            name_id = new_volume['_name_id'] or new_volume['id']
            provider_location = new_volume['provider_location']
            return {'_name_id': name_id,
                    'provider_location': provider_location}

        existing_name = self.generate_volume_name(new_volume)
        # wanted_name = self.generate_volume_name(volume)
        wanted_name = "volume-%s" % volume['id']
        vdisk_exist_flag = self._assistant.is_vdisk_defined(wanted_name)
        try:
            if not vdisk_exist_flag:
                self._assistant.rename_vdisk(str(existing_name),
                                             str(wanted_name))
            else:
                name_id = new_volume['_name_id'] or new_volume['id']
                provider_location = new_volume['provider_location']
        except exception.VolumeBackendAPIException:
            LOG.error(_('Unable to rename the logical volume '
                        'for volume: %s'), volume['id'])
            name_id = new_volume._name_id or new_volume.id
            provider_location = new_volume['provider_location']
        # If the back-end name(id) for the volume has been renamed,
        # it is OK for the volume to keep the original name(id) and there is
        # no need to use the column "_name_id" to establish the mapping
        # relationship between the volume id and the back-end volume
        # name(id).
        # Set the key "_name_id" to None for a successful rename.
        return {'_name_id': name_id, 'provider_location': provider_location}

    def manage_existing_snapshot_get_size(self, snapshot, existing_ref):
        """Return size of an existing image for manage_existing.

        :param snapshot:
            snapshot ref info to be set
        :param existing_ref:
            existing_ref is a dictionary of the form:
            {'source-name': <name of snapshot>}
        """
        # Check that the reference is valid
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        if 'source-name' not in existing_ref:
            reason = _('Reference must contain source-name element.')
            raise exception.ManageExistingInvalidReference(
                existing_ref=existing_ref, reason=reason)
        volume_name = self.generate_volume_name(snapshot.volume)
        snapshot_name = existing_ref['source-name']
        # get all snapshots of the volume
        snapshots_list, _ = self._assistant.get_vdisk_lc_mappings(
                                            volume_name, 'source')

        for snapshot_obj in snapshots_list:
            if (('target_vdisk_name' in snapshot_obj
                    and snapshot_name == snapshot_obj['target_vdisk_name'])
                or ('targetVdiskName' in snapshot_obj
                     and snapshot_name == snapshot_obj['targetVdiskName'])):
                snap_attr = self._assistant.get_vdisk_attributes(snapshot_name)
                return int(math.ceil(float(snap_attr['capacity']) / units.Gi))

        kwargs = {'existing_ref': snapshot_name,
                      'reason': 'Specified snapshot does not exist.'}
        raise exception.ManageExistingInvalidReference(**kwargs)

    def manage_existing_snapshot(self, snapshot, existing_ref):
        """Manages an existing snapshot.

        Renames the snapshot name to match the expected name for the snapshot.
        Error checking done by manage_existing_get_size is not repeated.

        :param snapshot:
            snapshot ref info to be set
        :param existing_ref:
            existing_ref is a dictionary of the form:
            {'source-name': <name of snapshot>}
        """
        if not isinstance(existing_ref, dict):
            existing_ref = {"source-name": existing_ref}
        volume_name = self.generate_volume_name(snapshot.volume)
        src_snapshot_name = existing_ref['source-name']
        dest_snapshot_name = self.generate_snapshot_name(snapshot)
        is_existed = False

        src, target = self._assistant.get_vdisk_lc_mappings(
                    src_snapshot_name, type='source')
        if src:
            # Exclude clone snapshot.
            msg = (_('Failed to manage existing snapshot %s,'
                    'due to being used for clone snap') % src_snapshot_name)
            raise exception.InvalidSnapshot(reason=msg)


        # get all snapshots of the volume
        snapshots_list, target = self._assistant.get_vdisk_lc_mappings(
                    volume_name, type='source')

        for snapshot_obj in snapshots_list:
            if (('target_vdisk_name' in snapshot_obj
                    and src_snapshot_name == snapshot_obj['target_vdisk_name'])
                or ('targetVdiskName' in snapshot_obj
                    and src_snapshot_name == snapshot_obj['targetVdiskName'])):
                is_existed = True
                if self._assistant.is_vdisk_in_use(src_snapshot_name):
                    msg = (('Failed to manage existing snapshot %s,'
                            'due to being in use') % src_snapshot_name)
                    raise exception.InvalidSnapshot(reason=msg)
                else:
                    break
        if not is_existed:
            msg = (_('Failed to manage existing snapshot %s,'
                     'due to snapshot is not existed') % src_snapshot_name)
            raise exception.InvalidSnapshot(reason=msg)
        if dest_snapshot_name != src_snapshot_name:
            self._assistant.rename_vdisk(str(src_snapshot_name),
                                             str(dest_snapshot_name))

    def get_manageable_snapshots(self, cinder_snapshots, marker, limit, offset,
                                 sort_keys, sort_dirs):
        """List manageable snapshots on AS18000 backend."""
        LOG.debug("Listing manageable snapshots")
        results = []
        cinder_snapshot_ids = [resource['id'] for resource in cinder_snapshots]
        new_volume_list = []

        for pool in self.configuration.instorage_mcs_volpool_name:
            volume_list = self._assistant.get_volumes(pool)
            for lun in volume_list:
                volume_tmp = {}
                vol_name = lun['name']
                # get all snapshots of the volume
                snapshots_list, _ = self._assistant.get_vdisk_lc_mappings(
                    vol_name, type='source')
                if not snapshots_list:
                    continue

                volume_tmp = {
                    'name': vol_name,
                    'pool': pool,
                    'snapshotlist': snapshots_list
                }
                new_volume_list.append(volume_tmp)

        for volume in new_volume_list:
            for snapshot in volume['snapshotlist']:
                if 'target_vdisk_name' in snapshot:
                    snapshot_name = snapshot['target_vdisk_name']
                else:
                    snapshot_name = snapshot['targetVdiskName']
                snap_attr = self._assistant.get_vdisk_attributes(snapshot_name)
                size = int(math.ceil(float(snap_attr['capacity']) / units.Gi))
                snapshot_info = {
                    'reference': {'source-name': snapshot_name},
                    'size': size,
                    'cinder_id': None,
                    'extra_info': {'pool': volume['pool']},
                    'source_reference': {'source-name': volume['name']}
                }
                src, _ = self._assistant.get_vdisk_lc_mappings(snapshot_name, 'source')
                if self._assistant.is_vdisk_in_use(snapshot_name):
                    # Exclude snapshots in use.
                    snapshot_info['safe_to_manage'] = False
                    snapshot_info['reason_not_safe'] = 'snapshot in use'
                elif (snapshot_name.startswith('snapshot-')
                        and (snapshot_name.replace('snapshot-', '') in cinder_snapshot_ids)):
                    # Exclude snapshots already managed.
                    snapshot_info['safe_to_manage'] = False
                    snapshot_info['reason_not_safe'] = 'already managed'
                    snapshot_info['cinder_id'] = snapshot_name.replace('snapshot-', '')
                elif src:
                    snapshot_info['safe_to_manage'] = False
                    snapshot_info['reason_not_safe'] = 'it is a cascade snapshot'
                else:
                    snapshot_info['safe_to_manage'] = True
                    snapshot_info['reason_not_safe'] = None

                results.append(snapshot_info)

        return volume_utils.paginate_entries_list(
            results, marker, limit, offset, sort_keys, sort_dirs)

    def unmanage_snapshot(self, snapshot):
        """Removes the specified snapshot from Cinder management."""
        pass

    def manage_existing(self, volume, ref):
        """Manages an existing vdisk.

        Renames the vdisk to match the expected name for the volume.
        Error checking done by manage_existing_get_size is not repeated -
        if we got here then we have a vdisk that isn't in use (or we don't
        care if it is in use.
        """
        # Check that the reference is valid
        vdisk = self._manage_input_check(ref)
        vdisk_io_grp = self._assistant.get_volume_io_group(vdisk['name'])
        if vdisk_io_grp not in self._state['available_iogrps']:
            msg = (_("Failed to manage existing volume due to "
                     "the volume to be managed is not in a valid "
                     "I/O group."))
            raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        is_aa_vdisk = self._assistant.is_aa_vdisk(vdisk['name'])
        ctxt = context.get_admin_context()
        ctxt.project_id = volume.project_id
        if not is_aa_vdisk and self._replica_enabled:
            # Add replication check
            rep_type = self._get_volume_replicated_type(ctxt, volume)
            vol_rep_type = None
            rel_info = self._assistant.get_relationship_info(vdisk['name'])
            if rel_info:
                vol_rep_type = rel_info['copy_type']
                aux_info = self._aux_backend_assistant.get_system_info()
                if rel_info['aux_cluster_id'] != aux_info['system_id']:
                    msg = (_("Failed to manage existing volume due to the aux "
                             "cluster for volume %(volume)s is %(aux_id)s. The "
                             "configured cluster id is %(cfg_id)s") %
                           {'volume': vdisk['name'],
                            'aux_id': rel_info['aux_cluster_id'],
                            'cfg_id': aux_info['system_id']})
                    raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if vol_rep_type != rep_type:
                msg = (_("Failed to manage existing volume due to "
                         "the replication type of the volume to be managed is "
                         "mismatch with the provided replication type."))
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        # check the pool
        pool = volume_utils.extract_host(volume['host'], 'pool')
        if (('mdisk_grp_name' in vdisk and vdisk['mdisk_grp_name'] != pool) or
                ('mdiskGrpName' in vdisk and vdisk['mdiskGrpName'] != pool)):
            msg = (_("Failed to manage existing volume due to the "
                     "pool of the volume to be managed does not "
                     "match the backend pool. Pool of the "
                     "volume to be managed is %(vdisk_pool)s. Pool "
                     "of the backend is %(backend_pool)s.") %
                   {'vdisk_pool': vdisk['mdisk_grp_name'],
                    'backend_pool': pool})
            raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        is_ssa_pool = self._assistant.is_ssa_pool(pool)

        if volume['volume_type_id']:
            opts = self._get_vdisk_params(
                volume['volume_type_id'],
                volume_metadata=volume.get('volume_metadata')
            )

            vdisk_copy = (self._assistant.get_vdisk_copy_attrs(vdisk['name'], '0') or
                          self._assistant.get_vdisk_copy_attrs(vdisk['name'], '1'))
            if not vdisk_copy:
                msg = (_("Failed to manage existing volume due to the "
                             "vdisk copy of %s can't to be found") % vdisk['name'] )
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if opts['aa'] and not is_aa_vdisk:
                msg = (_("Failed to manage existing volume due to "
                         "the volume to be managed is general, but "
                         "the volume type chosen is active-active."))
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if not opts['aa'] and is_aa_vdisk:
                msg = (_("Failed to manage existing volume due to "
                         "the volume to be managed is active-active, but "
                         "the volume type chosen is general."))
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if opts['aa'] and is_aa_vdisk:
                master, aux = self._assistant.get_volume_pool_iogrp_map(vdisk['name'])
                if self.active_active_pool_map[master['mdisk_grp_name']] != aux['mdisk_grp_name']:
                    msg = (_("Failed to manage existing volume due to "
                             "active-active master aux pool map not match configure set."))
                    raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

                if self.active_active_iogrp_map[master['IO_group_id']] != aux['IO_group_id']:
                    msg = (_("Failed to manage existing volume due to "
                             "active-active master aux iogrp map not match configure set."))
                    raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if (vdisk_copy['autoexpand'] == 'on' and
                    (opts['rsize'] == -1 or opts['rsize'] == 100)):
                msg = (_("Failed to manage existing volume due to "
                            "the volume to be managed is thin, but "
                            "the volume type chosen is thick."))
                self.message_api.create(
                        ctxt,
                        message_field.Action.MANAGE_VOLUME,
                        resource_uuid=volume.id,
                        detail=message_field.Detail.MANAGE_TYPE_MISMATCH
                )
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if not vdisk_copy['autoexpand'] and (opts['rsize'] != -1 and opts['rsize'] != 100):
                msg = (_("Failed to manage existing volume due to "
                            "the volume to be managed is thick, but "
                            "the volume type chosen is thin."))
                self.message_api.create(
                        ctxt,
                        message_field.Action.MANAGE_VOLUME,
                        resource_uuid=volume.id,
                        detail=message_field.Detail.MANAGE_TYPE_MISMATCH
                )
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if is_ssa_pool:
                if ((('deduped_copy' in vdisk_copy and vdisk_copy['deduped_copy'] == 'no') or
                     ('dedupedCopy' in vdisk_copy and vdisk_copy['dedupedCopy'] == 'no')) and
                      opts['deduped']):
                    msg = (_("Failed to manage existing volume due to the "
                             "volume to be managed is not deduped, but "
                             "the volume type chosen is deduped."))
                    raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

                if ((('deduped_copy' in vdisk_copy and vdisk_copy['deduped_copy'] == 'yes') or
                     ('dedupedCopy' in vdisk_copy and vdisk_copy['dedupedCopy'] == 'yes')) and
                       not opts['deduped']):
                    msg = (_("Failed to manage existing volume due to the "
                             "volume to be managed is deduped, but "
                             "the volume type chosen is not deduped."))
                    raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if ((('compressed_copy' in vdisk_copy and vdisk_copy['compressed_copy'] == 'no') or
                ('compressedCopy' in vdisk_copy and vdisk_copy['compressedCopy'] == 'no')) and
                    opts['compressed']):
                msg = (_("Failed to manage existing volume due to the "
                         "volume to be managed is not compress, but "
                         "the volume type chosen is compress."))
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if ((('compressed_copy' in vdisk_copy and vdisk_copy['compressed_copy'] == 'yes') or
                ('compressedCopy' in vdisk_copy and vdisk_copy['compressedCopy'] == 'yes')) and
                    not opts['compressed']):
                msg = (_("Failed to manage existing volume due to the "
                         "volume to be managed is compress, but "
                         "the volume type chosen is not compress."))
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

            if (vdisk_io_grp not in
                    instorage_cli.InStorageAssistant.get_valid_requested_io_groups(
                        self._state, opts)):
                msg = (_("Failed to manage existing volume due to "
                         "I/O group mismatch. The I/O group of the "
                         "volume to be managed is %(vdisk_iogrp)s. I/O group"
                         "of the chosen type is %(opt_iogrp)s.") %
                       {'vdisk_iogrp': vdisk['IO_group_name'],
                        'opt_iogrp': opts['iogrp']})
                raise exception.ManageExistingVolumeTypeMismatch(reason=msg)

        vol_name = self.generate_volume_name(volume)
        model_update = {}
        if vdisk['name'] != vol_name:
            self._assistant.rename_vdisk(vdisk['name'], vol_name)

        if not is_aa_vdisk and self._replica_enabled and vol_rep_type:
            aux_vol = instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name
            self._aux_backend_assistant.rename_vdisk(
                rel_info['aux_vdisk_name'], aux_vol)
            model_update = {'replication_status': 'enabled'}

        if self.icos_support:
            model_update.update({'is_active_active': opts['aa']})
            model_update['provision_type'] = 'thick' if opts['rsize'] in [-1,100] else 'thin'
        return model_update

    def manage_existing_get_size(self, volume, ref):
        """Return size of an existing Vdisk for manage_existing.

        existing_ref is a dictionary of the form:
        {'source-id': <uid of disk>} or
        {'source-name': <name of the disk>}

        Optional elements are:
          'manage_if_in_use':  True/False (default is False)
            If set to True, a volume will be managed even if it is currently
            attached to a host system.
        """

        # Check that the reference is valid
        vdisk = self._manage_input_check(ref)

        # Check if the disk is in use, if we need to.
        manage_if_in_use = ref.get('manage_if_in_use', False)
        if (not manage_if_in_use and
                self._assistant.is_vdisk_in_use(vdisk['name'])):
            reason = _('The specified vdisk is mapped to a host.')
            raise exception.ManageExistingInvalidReference(existing_ref=ref,
                                                           reason=reason)

        return int(math.ceil(float(vdisk['capacity']) / units.Gi))

    def get_manageable_volumes(self, cinder_volumes, marker, limit, offset,
                               sort_keys, sort_dirs):
        """List volumes on the backend available for management by Cinder.
        """
        LOG.debug("Listing manageable volumes")
        results = []
        PREFIX = "volume-"
        cinder_volume_ids = [vol['id'] for vol in cinder_volumes]

        for pool in self.configuration.instorage_mcs_volpool_name:
            volume_list = self._assistant.get_volumes(pool)
            for volume in volume_list:
                vol_name = volume['name']
                src_vol_info = self._assistant.get_vdisk_attributes(vol_name)
                if src_vol_info is None:
                    kwargs = {'reason': 'volume does not exist.'}
                    continue
                size = int(math.ceil(float(src_vol_info['capacity']) / units.Gi))
                rcrel = self._assistant.get_relationship_info(vol_name)
                volume_info = {
                    'reference': {'source-name': vol_name},
                    'size': size,
                    'cinder_id': None,
                    'extra_info': {'pool': pool}
                }
                if (vol_name.startswith(PREFIX)
                       and (vol_name.replace(PREFIX, '') in cinder_volume_ids)):
                    # Exclude already managed by cinder
                    volume_info['safe_to_manage'] = False
                    volume_info['reason_not_safe'] = 'already managed'
                    volume_info['cinder_id'] = vol_name.replace(PREFIX, '')
                elif self._assistant.is_vdisk_in_use(vol_name):
                    # Exclude volume in use
                    volume_info['safe_to_manage'] = False
                    volume_info['reason_not_safe'] = 'volume in use'
                elif (rcrel and (('consistency_group_name' in rcrel
                                       and rcrel['consistency_group_name'])
                                 or ('consistencyGroupName' in rcrel
                                       and rcrel['consistencyGroupName']))):
                    # Exclude already consistencygroup
                    volume_info['safe_to_manage'] = False
                    volume_info['reason_not_safe'] = 'belong consistencygroup'
                else:
                    volume_info['safe_to_manage'] = True
                    volume_info['reason_not_safe'] = None

                results.append(volume_info)

        page_results = volume_utils.paginate_entries_list(
            results, marker, limit, offset, sort_keys, sort_dirs)

        return page_results

    def unmanage(self, volume):
        """Remove the specified volume from Cinder management."""
        pass

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If we haven't gotten stats yet or 'refresh' is True,
        run update the stats first.
        """
        if not self._stats or refresh:
            self._update_volume_stats()

        return self._stats

    def get_volumes_used_size(self, volume=None):
        """get the volumes used size(ICOS private interface)"""
        pools = self.configuration.instorage_mcs_volpool_name
        volume_name = None
        result = {}

        if volume:
            volume_name = self.generate_volume_name(volume)
            resp = self._assistant.get_volumes_used_size(volume_name=volume_name)
            result.update(resp)
        else:
            for pool in pools:
                resp = self._assistant.get_volumes_used_size(pool=pool)
                result.update(resp)
        return result

    # ## Group method ## #
    def _check_group_supported(self, group):
        support_grps = ['consistent_group_snapshot_enabled',
                        'consistent_group_replication_enabled']
        supported_grp = False
        for grp_spec in support_grps:
            if volume_utils.is_group_a_type(group, grp_spec):
                supported_grp = True
                break
        if not supported_grp:
            LOG.warning('group: %s is not a supported group '
                      'type.', group.group_type_id)
            raise NotImplementedError()

    def create_group(self, context, group):
        """Create a group."""
        self._check_group_supported(group)
        if (volume_utils.is_group_a_type(group, 'consistent_group_snapshot_enabled')
                and volume_utils.is_group_a_type(group, 'consistent_group_replication_enabled')):
            LOG.error('Unable to create group: create group with mixed specs is'
                      'not supported.')
            model_update = {'status': 'error'}
            return model_update

        if volume_utils.is_group_a_type(group, 'consistent_group_snapshot_enabled'):
            return self.create_consistencygroup(context, group)

        if volume_utils.is_group_a_type(group, 'consistent_group_replication_enabled'):
            return self._create_replicationgroup(context, group)

    def _create_replicationgroup(self, cxt, group):
        """Create a replication group."""
        LOG.debug("Creating replication group.")
        model_update = {'status': 'available'}
        self._validate_replication_enabled()
        rccg_type = None
        for vol_type_id in group.volume_type_ids:
            replication_type = self._get_volume_replicated_type(
                cxt, None, vol_type_id)
            if not replication_type:
                # An unsupported configuration
                LOG.error('Unable to create group: create consistent '
                            'replication group with non-replication '
                            'volume type is not supported.')
                model_update = {'status': 'error'}
                return model_update
            if not rccg_type:
                rccg_type = replication_type
            elif rccg_type != replication_type:
                # An unsupported configuration
                LOG.error('Unable to create group: create consistent '
                            'replication group with different replication '
                            'type is not supported.')
                model_update = {'status': 'error'}
                return model_update
        group_name = self.generate_rccg_name(group)
        try:
            tgt_sys = self._aux_backend_assistant.get_system_info()
            self._assistant.create_rc_consistgrp(
                group_name, tgt_sys.get('system_id'))
            model_update.update({'replication_status': 'enabled'})
        except exception.VolumeBackendAPIException as ex:
            LOG.error("Failed to create rccg %(rccg)s. "
                        "Exception: %(exception)s.",
                        {'rccg': group_name, 'exception': ex})
            model_update = {'status': 'error'}
        return model_update


    def create_consistencygroup(self, context, group):
        """Create a consistencygroup.

        inspur InStorage will create group until group-snapshot creation,
        db will maintain the volumes and group relationship.
        """
        LOG.debug("Creating consistencygroup.")
        for vol_type_id in group.volume_type_ids:
            replication_type = self._get_volume_replicated_type(
                context, None, vol_type_id)
            if replication_type:
                # An unsupported configuration
                LOG.error('Unable to create group: create consistent snapshot '
                          'group with replication volume type is not supported.')
                model_update = {'status': 'error'}
                return model_update
        model_update = {'status': 'available'}
        return model_update

    def create_group_from_src(self, context, group, volumes,
                              group_snapshot=None, snapshots=None,
                              source_group=None, source_vols=None):
        """Creates a group from source.

        :param context: the context of the caller.
        :param group: the dictionary of the group to be created.
        :param volumes: a list of volume dictionaries in the group.
        :param group_snapshot: the dictionary of the group_snapshot as source.
        :param snapshots: a list of snapshot dictionaries
                in the group_snapshot.
        :param source_group: the dictionary of a group as source.
        :param source_vols: a list of volume dictionaries in the source_group.
        :returns: model_update, volumes_model_update
        """
        LOG.debug('Enter: create_group_from_src.')
        self._check_group_supported(group)

        return self.create_consistencygroup_from_src(
                   context, group, volumes, group_snapshot, snapshots,
                   source_group, source_vols)

    def create_consistencygroup_from_src(self, context, group, volumes,
                                         group_snapshot=None, snapshots=None,
                                         source_group=None, source_vols=None):
        if (self.configuration.instorage_mcs_fastconsistgrp_support
            and group_snapshot and snapshots):
            msg = (_('Fast localcopy consistgrp do not support creating groups '
                         'from group snapshot.'))
            raise exception.VolumeDriverException(message=msg)

        if group_snapshot and snapshots:
            group_name = 'lcgroup-' + group_snapshot.id
            sources = snapshots
            src_name_generator = self.generate_snapshot_name
        elif source_group and source_vols:
            group_name = 'lcgroup-' + source_group.id
            sources = source_vols
            src_name_generator = self.generate_volume_name
        else:
            error_msg = _("create_group_from_src must be creating from"
                          " a group snapshot, or a source group.")
            raise exception.InvalidInput(reason=error_msg)

        tgt_name_generator = self.generate_volume_name

        LOG.debug('create_group_from_src: group_name %(group_name)s'
                  ' %(sources)s', {'group_name': group_name,
                                   'sources': sources})
        self._assistant.create_lc_consistgrp(group_name, self.configuration)
        timeout = self.configuration.instorage_mcs_localcopy_timeout
        model_update, volumes_model = (
            self._assistant.create_group_from_source(group, group_name,
                                                     sources, volumes,
                                                     self._state,
                                                     self.configuration,
                                                     timeout,
                                                     src_name_generator,
                                                     tgt_name_generator))
        if volume_utils.is_group_a_type(
                group, 'consistent_group_replication_enabled'):
            self._validate_replication_enabled()
            rccg_name = self.generate_rccg_name(group)
            try:
                tgt_sys = self._aux_backend_assistant.get_system_info()
                self._assistant.create_rc_consistgrp(
                    rccg_name, tgt_sys.get('system_id'))
                model_update.update({'replication_status': 'enabled'})
            except exception.VolumeBackendAPIException as ex:
                LOG.error("Failed to create rccg %(rccg)s. "
                            "Exception: %(exception)s.",
                            {'rccg': rccg_name, 'exception': ex})
                model_update = {'status': 'error'}

        for vol in volumes:
            rep_type = self._get_volume_replicated_type(context, vol)
            volume_model = dict()
            for model in volumes_model:
                if vol.id == model["id"]:
                    volume_model = model
                    break
            if rep_type:
                replica_obj = self._get_replica_obj(rep_type)
                replica_obj.volume_replication_setup(context, vol)
                volume_model['replication_status'] = 'enabled'

            opts = self._get_vdisk_params(
            vol['volume_type_id'],
            volume_metadata=vol.get('volume_metadata'))
            if opts['qos']:
                vol_name = self.generate_volume_name(vol)
                self._assistant.add_vdisk_qos(vol_name, opts['qos'])

        if volume_utils.is_group_a_type(
                group, 'consistent_group_replication_enabled'):
            model_update, added_vols, removed_vols = (
                self._update_replication_grp(context, group, volumes, []))
            model_update.update({'replication_status': 'enabled'})

        LOG.debug("Leave: create_group_from_src.")
        return model_update, volumes_model

    def delete_group(self, context, group, volumes):
        """Deletes a group.

        inspur InStorage will delete the volumes of the group.
        """
        self._check_group_supported(group)
        if volume_utils.is_group_a_type(
                group, 'consistent_group_replication_enabled'):
            return self._delete_replication_grp(group, volumes)

        if volume_utils.is_group_a_type(
                group, 'consistent_group_snapshot_enabled'):
            return self.delete_consistencygroup(context, group, volumes)

    def _delete_replication_grp(self, group, volumes):
        LOG.debug("Deleting replication group.")
        model_update = {'status': 'deleted'}
        volumes_model_update = []
        group_name = self.generate_rccg_name(group)
        try:
            self._assistant.delete_rc_consistgrp(group_name)
        except exception.VolumeBackendAPIException as ex:
            LOG.error("Failed to delete rccg %(rccg)s. "
                      "Exception: %(exception)s.",
                      {'rccg': group_name, 'exception': ex})
            model_update = {'status': 'error_deleting'}

        for volume in volumes:
            try:
                vol_name = self.generate_volume_name(volume)
                self._aux_backend_assistant.delete_rc_volume(vol_name,
                                                            target_vol=True)

                self._local_backend_assistant.delete_rc_volume(vol_name)
                volumes_model_update.append(
                    {'id':volume.id, 'status': 'deleted'})
            except exception.VolumeBackendAPIException as ex:
                model_update['status'] = 'error_deleting'
                LOG.error(_('Failed to get delete volume %(volume)s '
                            'Exception: %(err)s.'),
                            {'volume': vol_name,
                            'err': six.text_type(ex)})
                volumes_model_update.append(
                    {'id':volume.id, 'status': 'error_deleting'})

        return model_update, volumes_model_update

    def delete_consistencygroup(self, context, group, volumes):
        LOG.debug("Deleting consistencygroup.")
        model_update = {'status': 'deleted'}
        volumes_model_update = []

        for volume in volumes:
            vol_name = self.generate_volume_name(volume)
            try:
                self._assistant.delete_vdisk(vol_name, True)
                volumes_model_update.append(
                    {'id': volume['id'], 'status': 'deleted'})
            except exception.VolumeBackendAPIException as err:
                model_update['status'] = 'error_deleting'
                LOG.error(_("Failed to delete the volume %(vol)s of group. "
                            "Exception: %(exception)s."),
                          {'vol': vol_name, 'exception': err})
                volumes_model_update.append(
                    {'id': volume['id'], 'status': 'error_deleting'})

        return model_update, volumes_model_update

    def update_group(self, ctxt, group, add_volumes=None,
                     remove_volumes=None):
        """Adds or removes volume(s) to/from an existing group."""
        self._check_group_supported(group)
        if volume_utils.is_group_a_type(
                group, 'consistent_group_replication_enabled'):
            return self._update_replication_grp(
                   ctxt, group, add_volumes, remove_volumes)

        if volume_utils.is_group_a_type(
                group, 'consistent_group_snapshot_enabled'):
            return self.update_consistencygroup(
                   ctxt, group, add_volumes, remove_volumes)

    def _update_replication_grp(self, context, group,
                                add_volumes=None, remove_volumes=None):
        LOG.debug("Updating replication group.")
        model_update = {'status': 'available'}
        group_name = self.generate_rccg_name(group)
        try:
            rccg = self._assistant.get_remotecopy_consistgrp_attr(group_name)
        except Exception as exp:
            if add_volumes:
                LOG.exeception("Unable to retrieve replication group infomation."
                               "Failed with exception %(exp)s", exp)
        if not rccg and len(add_volumes) > 0:
            LOG.error("Failed to update group: %(grp)s does not exist in backend.",
                      {'grp':group.id})
            model_update['status'] = 'error'
            return model_update, None, None

        # add remote copy relationship to rccg
        added_vols = []
        for volume in add_volumes:
            try:
                vol_name = None
                if not self._active_backend_id:
                    vol_name = self.generate_volume_name(volume)
                else:
                    vol_name = (instorage_const.REPLICA_AUX_VOL_PREFIX
                                + self.generate_volume_name(volume))
                rcrel = self._assistant.get_relationship_info(vol_name)
                if not rcrel:
                    LOG.error("Failed to update group: remote copy relationship"
                            " of %(vol)s does not exist in backend",
                            {'vol':vol_name})
                    model_update['status'] = 'error'
                    break
                else:
                    if(rccg['copy_type'] != 'empty_group' and
                    any(k for k in ('copy_type', 'state', 'primary')
                        if rccg[k] != rcrel[k])):
                        LOG.error("Failed to update rccg %(rccg)s: remote copy "
                                "type of %(vol)s is %(vol_rc_type)s, the rccg "
                                "type is %(rccg_type)s. rcrel state is ",
                                "%(rcrel_state)s rccg state is %(rccg_state)s, "
                                "the rcrel primary is %(rcrel_primary)s, "
                                "the rccg primary is %(rccg_primary)s.",
                                {'rccg':group_name,
                                'vol':volume.id,
                                'vol_rc_type':rcrel['copy_type'],
                                'rccg_type':rccg['copy_type'],
                                'rcrel_state':rcrel['state'],
                                'rccg_state':rccg['state'],
                                'rcrel_primary':rcrel['primary'],
                                'rccg_primary':rccg['primary']})
                        #self._assistant.start_relationship(vol_name)
                        model_update['status'] = 'error'
                    else:
                        self._assistant.chrcrelationship(
                            'name', rcrel['name'], group_name)
                        if rccg['copy_type'] == 'empty_group':
                            rccg = self._assistant.get_remotecopy_consistgrp_attr(
                                group_name)
                        added_vols.append({'id': volume.id, 'group_id': group.id})
                        self._update_rccg_properties(context, volume, group)

            except exception.VolumeBackendAPIException as ex:
                model_update['status'] = 'error'
                LOG.error("Failed to add the remote copy of volume %(vol)s to"
                          " group.Exception: %(exeception)s.",
                          {'vol':vol_name, 'exeception': ex})
        # Remove remote copy relationship from rccg
        removed_vols = []
        for volume in remove_volumes:
            try:
                vol_name = None
                if not self._active_backend_id:
                    vol_name = self.generate_volume_name(volume)
                else:
                    vol_name = (instorage_const.REPLICA_AUX_VOL_PREFIX
                                + self.generate_volume_name(volume))
                rcrel = self._assistant.get_relationship_info(vol_name)
                if not rcrel:
                    LOG.error("Failed to update group: remote copy relationship"
                            " of %(vol)s does not exist in backend",
                            {'vol':vol_name})
                    model_update['status'] = 'error'
                    break
                else:
                    self._assistant.chrcrelationship('name', rcrel['name'])
                    removed_vols.append({'id': volume.id, 'group_id': None})
                    self._update_rccg_properties(context, volume)

            except exception.VolumeBackendAPIException as ex:
                model_update['status'] = 'error'
                LOG.error("Failed to remove the remote copy of volume %(vol)s "
                          "from group.Exception: %(exeception)s.",
                          {'vol':vol_name, 'exeception': ex})

        return model_update, added_vols, removed_vols

    def _update_rccg_properties(self, ctxt, volume, group=None):
        group_name = None
        if group:
            group_name = self.generate_rccg_name(group)
        else:
            group_name = ""
        if not volume.metadata:
            volume.metadata = dict()
        volume.metadata['Consistency Group Name'] = group_name
        volume.save()

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
        LOG.debug("Updating consistencygroup.")
        return None, None, None

    def create_group_snapshot(self, ctxt, group_snapshot, snapshots):
        """Creates a group_snapshot.

        :param context: the context of the caller.
        :param group_snapshot: the GroupSnapshot object to be created.
        :param snapshots: a list of Snapshot objects in the group_snapshot.
        :returns: model_update, snapshots_model_update
        """
        self._check_group_supported(group_snapshot)
        return self.create_cgsnapshot(ctxt, group_snapshot, snapshots)

    def create_cgsnapshot(self, ctxt, group_snapshot, snapshots):
        # Use cgsnapshot id as cg name
        group_name = 'group_snap-' + group_snapshot.id
        # Create new cg as cg_snapshot
        self._assistant.create_lc_consistgrp(group_name, self.configuration)

        timeout = self.configuration.instorage_mcs_localcopy_timeout
        model_update, snapshots_model = (
            self._assistant.run_group_snapshots(group_name,
                                                snapshots,
                                                self._state,
                                                self.configuration,
                                                timeout,
                                                self.generate_snapshot_name,
                                                self.generate_volume_name))

        return model_update, snapshots_model

    def delete_group_snapshot(self, context, group_snapshot, snapshots):
        """Deletes a group_snapshot.

        :param context: the context of the caller.
        :param group_snapshot: the GroupSnapshot object to be deleted.
        :param snapshots: a list of snapshot objects in the group_snapshot.
        :returns: model_update, snapshots_model_update
        """
        self._check_group_supported(group_snapshot)
        return self.delete_cgsnapshot(context, group_snapshot, snapshots)

    def delete_cgsnapshot(self, context, group_snapshot, snapshots):
        group_snapshot_id = group_snapshot['id']
        group_name = 'group_snap-' + group_snapshot_id
        model_update, snapshots_model = (
            self._assistant.delete_group_snapshots(group_name,
                                                   snapshots,
                                                   self.generate_snapshot_name))

        return model_update, snapshots_model

    def get_pool(self, volume):
        vol_name = self.generate_volume_name(volume)
        attr = self._assistant.get_vdisk_attributes(vol_name)

        if attr is None:
            msg = (_('get_pool: Failed to get attributes for volume '
                     '%s') % volume['id'])
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        return attr['mdisk_grp_name']

    def _check_aa_alive(self):
        online_nodes = self._assistant.get_node_info()
        available_io_groups = set(node['IO_group']
                                  for __, node in online_nodes.items())
        config_io_groups = []
        if self.active_active_iogrp_map:
            for primary, second in self.active_active_iogrp_map.items():
                config_io_groups.append(primary)
                config_io_groups.append(second)
        config_io_groups = set(config_io_groups)
        if not config_io_groups.issubset(available_io_groups):
            msg = _('_check_aa_alive failed, available_io_groups: %s, '
                    'config_io_groups: %s') % (available_io_groups, config_io_groups)
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""

        LOG.debug("Updating volume stats.")
        if self.configuration.safe_get('instorage_mcs_enable_aa'):
            self._check_aa_alive()

        data = {}

        data['vendor_name'] = self.VENDOR
        data['driver_version'] = self.VERSION
        data['storage_protocol'] = self.protocol
        data['pools'] = []

        backend_name = self.configuration.safe_get('volume_backend_name')
        data['volume_backend_name'] = (backend_name or
                                       self._state['system_name'])

        data['pools'] = [self._build_pool_stats(pool)
                         for pool in
                         self._get_backend_pools()]
        if self._replica_enabled:
            data['replication'] = self._replica_enabled
            data['replication_enabled'] = self._replica_enabled
            data['replication_targets'] = self._get_replication_targets()
            data['consistent_group_replication_enabled'] = True
        self._stats = data

    def _build_pool_stats(self, pool):
        """Build pool status"""
        qos_support = True
        pool_stats = {}
        try:
            pool_data = self._assistant.get_pool_attrs(pool)
            if pool_data:
                in_tier = pool_data['in_tier'] in ['on', 'auto']
                total_capacity_gb = float(pool_data['capacity']) / units.Gi
                free_capacity_gb = float(pool_data['free_capacity']) / units.Gi
                allocated_capacity_gb = (float(pool_data['used_capacity']) /
                                         units.Gi)
                if ('snapshot_used_capacity' in pool_data
                        and 'nonsnapshot_virtual_capacity' in pool_data):
                    provisioned_capacity_gb = (float(pool_data['snapshot_used_capacity']) / units.Gi
                        + float(pool_data['nonsnapshot_virtual_capacity']) / units.Gi)
                else:
                    provisioned_capacity_gb = float(
                        pool_data['virtual_capacity']) / units.Gi

                deduped_support = (self._state['deduped_enabled'] and
                                   'category' in pool_data and
                                   pool_data['category'] == 'ssa')
                active_active_enabled = self.configuration.safe_get('instorage_mcs_enable_aa')
                if active_active_enabled and not self._state['lcmap_remotecopy_extend_support']:
                    online_extend_support = False
                else:
                    online_extend_support = True
                over_sub_ratio = self.configuration.safe_get(
                    'max_over_subscription_ratio')
                location_info = ('InStorageMCSDriver:%(sys_id)s:%(pool)s' %
                                 {'sys_id': self._state['system_id'],
                                  'pool': pool_data['name']})
                thick_provisioning_support = True
                if 'category' in pool_data and pool_data['category'] == 'ssa':
                    thick_provisioning_support = False
                pool_stats = {
                    'pool_name': pool_data['name'],
                    'total_capacity_gb': total_capacity_gb,
                    'free_capacity_gb': free_capacity_gb,
                    'allocated_capacity_gb': allocated_capacity_gb,
                    'provisioned_capacity_gb': provisioned_capacity_gb,
                    'compressed': self._state['compression_enabled'],
                    'deduped': deduped_support,
                    'reserved_percentage':
                        self.configuration.reserved_percentage,
                    'QoS_support': qos_support,
                    'consistencygroup_support': True,
                    'consistent_group_snapshot_enabled': True,
                    'location_info': location_info,
                    'intier_support': in_tier,
                    'multiattach': True,
                    'thin_provisioning_support': True,
                    'thick_provisioning_support': thick_provisioning_support,
                    'max_over_subscription_ratio': over_sub_ratio,
                    'online_extend_support': online_extend_support,
                    'ip_addr': self.active_ip,
                    'active_active_enabled':active_active_enabled
                }
            if self._replica_enabled:
                pool_stats.update({
                    'replication_enabled': self._replica_enabled,
                    'replication_type': self._supported_replica_types,
                    'replication_targets': self._get_replication_targets(),
                    'replication_count': len(self._get_replication_targets()),
                    'consistent_group_replication_enabled': True
                })

        except exception.VolumeBackendAPIException:
            msg = _('Failed getting details for pool %s.') % pool
            raise exception.VolumeBackendAPIException(data=msg)

        if self.icos_support:
            resp_info = self._assistant.get_cluster_capacity_info()
            total_mdisk_capacity_gb =(
                math.floor(float(resp_info['total_mdisk_capacity']) / units.Gi))
            total_allocated_extent_capacity_gb = math.floor(float(
                resp_info['total_allocated_extent_capacity']) / units.Gi)
            total_vdiskcopy_capacity_gb =(
                math.floor(float(resp_info['total_vdiskcopy_capacity']) / units.Gi))
            total_free_capacity_gb = math.floor((float(total_mdisk_capacity_gb -
                                                    total_allocated_extent_capacity_gb)))

            if total_mdisk_capacity_gb and total_free_capacity_gb and total_vdiskcopy_capacity_gb:
                pool_stats['inspur_total_capacity'] = total_mdisk_capacity_gb
                pool_stats['inspur_free_capacity'] = total_free_capacity_gb
                pool_stats['inspur_provisioned_capacity'] = total_vdiskcopy_capacity_gb

        return pool_stats

    def _get_replication_targets(self):
        return [self._replica_target['backend_id']]

    def _manage_input_check(self, ref):
        """Verify the input of manage function."""
        # Check that the reference is valid
        if 'source-name' in ref:
            manage_source = ref['source-name']
            zhpattern = re.compile(r'[\u4e00-\u9fa5]')
            if manage_source.isdigit() or zhpattern.search(manage_source) or\
                    not manage_source.strip('-'):
                vdisk = None
            else:
                vdisk = self._assistant.get_vdisk_attributes(manage_source)
        elif 'source-id' in ref:
            manage_source = ref['source-id']
            vdisk = self._assistant.vdisk_by_uid(manage_source)
        else:
            reason = _('Reference must contain source-id or '
                       'source-name element.')
            raise exception.ManageExistingInvalidReference(existing_ref=ref,
                                                           reason=reason)

        if vdisk is None:
            reason = (_('No vdisk with the UID specified by ref %s.')
                      % manage_source)
            raise exception.ManageExistingInvalidReference(existing_ref=ref,
                                                           reason=reason)
        return vdisk

    # OpenStack Pike introduced replication group support
    def failover_host(self, context, volumes, secondary_id=None, groups=None):
        LOG.debug('enter: failover_host: secondary_id=%(id)s',
                  {'id': secondary_id})
        if not self._replica_enabled:
            msg = _("Replication is not properly enabled on backend.")
            LOG.error(msg)
            raise exception.UnableToFailOver(reason=msg)

        if instorage_const.FAILBACK_VALUE == secondary_id:
            # In this case the administrator would like to fail back.
            secondary_id, volumes_update, groups_update = self._replication_failback(
                context, volumes, groups)
        elif (secondary_id == self._replica_target['backend_id'] or
              secondary_id is None):
            # In this case the administrator would like to fail over.
            secondary_id, volumes_update, groups_update = self._replication_failover(
                context, volumes, groups)
        else:
            msg = (_("Invalid secondary id %s.") % secondary_id)
            LOG.error(msg)
            raise exception.InvalidReplicationTarget(reason=msg)

        LOG.debug('leave: failover_host: secondary_id=%(id)s',
                  {'id': secondary_id})
        if groups is None:
            return secondary_id, volumes_update
        else:
            return secondary_id, volumes_update, groups_update

    def enable_replication(self, context, group, volumes):
        """Enables replication for a group and volumes in the group."""
        model_update = {'replication_status': 'enabled'}
        volumes_update = []
        rccg_name = self.generate_rccg_name(group)
        rccg = self._assistant.get_remotecopy_consistgrp_attr(rccg_name)
        if rccg and rccg['relationship_count'] != '0':
            try:
                if rccg['primary'] == 'aux':
                    self._assistant.start_rccg(rccg_name, primary='aux')
                else:
                    self._assistant.start_rccg(rccg_name, primary='master')
            except exception.VolumeBackendAPIException as exp:
                LOG.error("Failed to enable group replication on %(rccg)s. "
                          "Exception: %(exception)s.",
                          {'rccg': rccg_name, 'exception': exp})
                model_update['replication_status'] = 'error'
        else:
            if rccg:
                LOG.error("Enable replication on empty group %(rccg)s is "
                          "forbidden.", {'rccg': rccg['name']})
            else:
                LOG.error("Failed to enable group replication: %(grp)s does "
                          "not exist in backend.", {'grp': group.id})
            model_update['replication_status'] = 'error'

        for volume in volumes:
            volumes_update.append(
                {'id': volume.id,
                 'replication_status': model_update['replication_status']})
        return model_update, volumes_update

    def disable_replication(self, context, group, volumes):
        """Disables replication for a group and volumes in the group."""
        model_update = {'replication_status': 'disabled'}
        volumes_update = []
        rccg_name = self.generate_rccg_name(group)
        rccg = self._assistant.get_remotecopy_consistgrp_attr(rccg_name)
        if rccg and rccg['relationship_count'] != '0':
            try:
                self._assistant.stop_rccg(rccg_name)
            except exception.VolumeBackendAPIException as exp:
                LOG.error("Failed to disable group replication on %(rccg)s. "
                          "Exception: %(exception)s.",
                          {'rccg': rccg_name, 'exception': exp})
                model_update['replication_status'] ='error'
        else:
            if rccg:
                LOG.error("Disable replication on empty group %(rccg)s is "
                          "forbidden.", {'rccg': rccg['name']})
            else:
                LOG.error("Failed to disable group replication: %(grp)s does "
                          "not exist in backend.", {'grp': group.id})
            model_update['replication_status'] = 'error'

        for volume in volumes:
            volumes_update.append(
                {'id': volume.id,
                 'replication_status': model_update['replication_status']})
        return model_update, volumes_update

    def failover_replication(self, context, group, volumes,
                             secondary_backend_id=None):
        """Fails over replication for a group and volumes in the group."""
        volumes_model_update = []
        model_update = {}
        if not self._replica_enabled:
            msg = _("Replication is not properly enabled on backend.")
            LOG.error(msg)
            raise exception.UnableToFailOver(reason=msg)

        if instorage_const.FAILBACK_VALUE == secondary_backend_id:
            # In this case the administrator would like to group fail back.
            self._sync_replica_groups(context, [group])
            self._wait_replica_groups_ready(context, [group])
            model_update = self._rep_grp_failback(context, group)
        elif (secondary_backend_id == self._replica_target['backend_id'] or
                secondary_backend_id is None):
            # In this case the administrator would like to group fail over.
            model_update = self._rep_grp_failover(context, group)
        else:
            msg = (_("Invalid secondary id %s.") % secondary_backend_id)
            LOG.error(msg)
            raise exception.InvalidReplicationTarget(reason=msg)

        for vol in volumes:
            volume_model_update = {'id': vol.id,
                                   'replication_status':
                                       model_update['replication_status']}
            volumes_model_update.append(volume_model_update)
        return model_update, volumes_model_update

    def _rep_grp_failback(self, ctxt, group, sync_grp=True):
        """Fail back all the volume in the replication group."""
        model_update = {
            'replication_status': 'enabled'}
        rccg_name = self.generate_rccg_name(group)

        try:
            self._aux_backend_assistant.stop_rc_consistgrp(
                rccg_name, access=True)
            self._aux_backend_assistant.start_rc_consistgrp(
                rccg_name, primary='master')
            return model_update
        except exception.VolumeBackendAPIException as exp:
            msg = (('Unable to fail back the group %(rccg)s, error: '
                     '%(error)s') % {"rccg": rccg_name, "error": exp})
            LOG.exception(msg)
            raise exception.UnableToFailOver(reason=msg)

    def _rep_grp_failover(self, ctxt, group):
        """Fail over all the volume in the replication group."""
        model_update = {
            'replication_status': 'failed-over'}
        rccg_name = self.generate_rccg_name(group)

        try:
            self._aux_backend_assistant.stop_rccg(rccg_name, access=True)
            self._assistant.start_rccg(rccg_name, primary='aux')
            return model_update
        except exception.VolumeBackendAPIException as exp:
            msg = (('Unable to fail over the group %(rccg)s to the aux '
                     'back-end, error: %(error)s') %
                   {"rccg": rccg_name, "error": exp})
            LOG.exception(msg)
            raise exception.UnableToFailOver(reason=msg)

    def _replication_failback(self, ctxt, volumes, groups):
        """Fail back all the volume on the secondary backend."""
        volumes_update = []
        groups_update = []
        if not self._active_backend_id:
            LOG.info(_("Host has been failed back. doesn't need "
                       "to fail back again"))
            return None, volumes_update, groups_update

        try:
            self._local_backend_assistant.get_system_info()
        except Exception:
            msg = (_("Unable to failback due to primary is not reachable."))
            LOG.error(msg)
            raise exception.UnableToFailOver(reason=msg)

        normal_volumes, rep_volumes = self._classify_volume(ctxt, volumes)

        # start synchronize from aux volume to master volume
        self._sync_with_aux(ctxt, rep_volumes)
        self._sync_replica_groups(ctxt, groups)
        self._wait_replica_ready(ctxt, rep_volumes)
        self._wait_replica_groups_ready(ctxt, groups)

        rep_volumes_update = self._failback_replica_volumes(ctxt,
                                                            rep_volumes)
        volumes_update.extend(rep_volumes_update)

        rep_vols_in_grp_update, groups_update = self._failback_replica_groups(
            ctxt, groups)
        volumes_update.extend(rep_vols_in_grp_update)

        normal_volumes_update = self._failback_normal_volumes(normal_volumes)
        volumes_update.extend(normal_volumes_update)

        self._assistant = self._local_backend_assistant
        self._active_backend_id = None

        # Update the instorage state
        self._update_instorage_state()
        self._update_volume_stats()
        return instorage_const.FAILBACK_VALUE, volumes_update, groups_update

    def _failback_replica_volumes(self, ctxt, rep_volumes):
        LOG.debug('enter: _failback_replica_volumes')
        volumes_update = []

        for volume in rep_volumes:
            vol_name = self.generate_volume_name(volume)
            rep_type = self._get_volume_replicated_type(ctxt, volume)
            replica_obj = self._get_replica_obj(rep_type)
            tgt_volume = instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name
            rep_info = self._assistant.get_relationship_info(tgt_volume)
            if not rep_info:
                volumes_update.append(
                    {'volume_id': volume['id'],
                     'updates': {'replication_status': 'error',
                                 'status': 'error'}})
                LOG.error(_('_failback_replica_volumes:no rc-releationship '
                            'is established between master: %(master)s and '
                            'aux %(aux)s. Please re-establish the '
                            'relationship and synchronize the volumes on '
                            'backend storage.'),
                          {'master': vol_name, 'aux': tgt_volume})
                continue
            LOG.debug('_failover_replica_volumes: vol=%(vol)s, master_vol='
                      '%(master_vol)s, aux_vol=%(aux_vol)s, state=%(state)s'
                      'primary=%(primary)s',
                      {'vol': vol_name,
                       'master_vol': rep_info['master_vdisk_name'],
                       'aux_vol': rep_info['aux_vdisk_name'],
                       'state': rep_info['state'],
                       'primary': rep_info['primary']})
            try:
                replica_obj.replication_failback(volume)
                model_updates = {'replication_status':'enabled'}
                volumes_update.append(
                    {'volume_id': volume['id'],
                     'updates': model_updates})
            except exception.VolumeDriverException:
                LOG.error(_('Unable to fail back volume %(volume_id)s'),
                          {'volume_id': volume.id})
                volumes_update.append(
                    {'volume_id': volume['id'],
                     'updates': {'replication_status': 'error',
                                 'status': 'error'}})
        LOG.debug('leave: _failback_replica_volumes '
                  'volumes_update=%(volumes_update)s',
                  {'volumes_update': volumes_update})
        return volumes_update

    def _failback_normal_volumes(self, normal_volumes):
        volumes_update = []
        for vol in normal_volumes:
            pre_status = 'available'
            if ('replication_driver_data' in vol and
                    vol['replication_driver_data']):
                rep_data = json.loads(vol['replication_driver_data'])
                pre_status = rep_data['previous_status']
            volumes_update.append(
                {'volume_id': vol['id'],
                 'updates': {'status': pre_status,
                             'replication_driver_data': ''}})
        return volumes_update

    def _sync_with_aux(self, ctxt, volumes):
        LOG.debug('enter: _sync_with_aux ')
        try:
            rep_mgr = self._get_replica_mgr()
            rep_mgr.establish_target_partnership()
        except Exception as ex:
            LOG.warning(_('Fail to establish partnership in backend. '
                          'error=%(ex)s'), {'error': six.text_type(ex)})
        for volume in volumes:
            vol_name = self.generate_volume_name(volume)
            tgt_volume = instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name
            rep_info = self._assistant.get_relationship_info(tgt_volume)
            if not rep_info:
                LOG.error(_('_sync_with_aux: no rc-releationship is '
                            'established between master: %(master)s and aux '
                            '%(aux)s. Please re-establish the relationship '
                            'and synchronize the volumes on backend '
                            'storage.'), {'master': vol_name,
                                          'aux': tgt_volume})
                continue
            LOG.debug('_sync_with_aux: volume: %(volume)s rep_info:master_vol='
                      '%(master_vol)s, aux_vol=%(aux_vol)s, state=%(state)s, '
                      'primary=%(primary)s',
                      {'volume': vol_name,
                       'master_vol': rep_info['master_vdisk_name'],
                       'aux_vol': rep_info['aux_vdisk_name'],
                       'state': rep_info['state'],
                       'primary': rep_info['primary']})
            try:
                if rep_info['state'] != instorage_const.REP_CONSIS_SYNC:
                    if rep_info['primary'] == 'master':
                        self._assistant.start_relationship(tgt_volume)
                    else:
                        self._assistant.start_relationship(tgt_volume,
                                                           primary='aux')
            except Exception as ex:
                LOG.warning(_('Fail to copy data from aux to master. master:'
                              ' %(master)s and aux %(aux)s. Please '
                              're-establish the relationship and synchronize'
                              ' the volumes on backend storage. error='
                              '%(ex)s'), {'master': vol_name,
                                          'aux': tgt_volume,
                                          'error': six.text_type(ex)})
        LOG.debug('leave: _sync_with_aux.')

    def _wait_replica_ready(self, ctxt, volumes):
        for volume in volumes:
            vol_name = self.generate_volume_name(volume)
            tgt_volume = instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name
            try:
                self._wait_replica_vol_ready(ctxt, tgt_volume)
            except Exception as ex:
                LOG.error(_('_wait_replica_ready: wait for volume:%(volume)s'
                            ' remote copy synchronization failed due to '
                            'error:%(err)s.'), {'volume': tgt_volume,
                                                'err': six.text_type(ex)})

    def _wait_replica_vol_ready(self, ctxt, volume):
        LOG.debug('enter: _wait_replica_vol_ready: volume=%(volume)s',
                  {'volume': volume})

        def _replica_vol_ready():
            rep_info = self._assistant.get_relationship_info(volume)
            if not rep_info:
                msg = (_('_wait_replica_vol_ready: no rc-releationship'
                         'is established for volume:%(volume)s. Please '
                         're-establish the rc-relationship and '
                         'synchronize the volumes on backend storage.'),
                       {'volume': volume})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            LOG.debug('_replica_vol_ready:volume: %(volume)s rep_info: '
                      'master_vol=%(master_vol)s, aux_vol=%(aux_vol)s, '
                      'state=%(state)s, primary=%(primary)s',
                      {'volume': volume,
                       'master_vol': rep_info['master_vdisk_name'],
                       'aux_vol': rep_info['aux_vdisk_name'],
                       'state': rep_info['state'],
                       'primary': rep_info['primary']})
            if rep_info['state'] == instorage_const.REP_CONSIS_SYNC:
                return True
            if rep_info['state'] == instorage_const.REP_IDL_DISC:
                msg = (_('Wait synchronize failed. volume: %(volume)s'),
                       {'volume': volume})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            return False

        self._assistant.wait_for_a_condition(
            _replica_vol_ready, timeout=instorage_const.DEFAULT_RC_TIMEOUT,
            interval=instorage_const.DEFAULT_RC_INTERVAL,
            raise_exception=True)
        LOG.debug('leave: _wait_replica_vol_ready: volume=%(volume)s',
                  {'volume': volume})

    def _failback_replica_groups(self, ctxt, groups):
        volumes_update = []
        groups_update = []
        for grp in groups:
            try:
                grp_rep_status = self._rep_grp_failback(
                    ctxt, grp, sync_grp=False)['replication_status']
            except Exception as ex:
                LOG.error('Fail to failback group %(grp)s during host '
                          'failback due to error: %(error)s',
                          {'grp': grp.id, 'error': ex})
                grp_rep_status = 'error'

            # Update all the volumes' status in that group
            for vol in grp.volumes:
                vol_update = {'volume_id': vol.id,
                              'updates':
                                  {'replication_status': grp_rep_status,
                                   'status': (
                                       vol.status if grp_rep_status ==
                                       'enabled' else 'error')}}
                volumes_update.append(vol_update)
            grp_status = ('available' if grp_rep_status == 'enabled'
                           else 'error')
            grp_update = {'group_id': grp.id,
                          'updates': {'replication_status': grp_rep_status,
                                      'status': grp_status}}
            groups_update.append(grp_update)
        return volumes_update, groups_update

    def _sync_replica_groups(self, ctxt, groups):
        for grp in groups:
            rccg_name = self.generate_rccg_name(grp)
            self._sync_with_aux_grp(ctxt, rccg_name)

    def _sync_with_aux_grp(self, ctxt, rccg_name):
        try:
            rccg = self._assistant.get_remotecopy_consistgrp_attr(rccg_name)
            if rccg and rccg['relationship_count'] != '0':
                if (rccg['state'] not in
                        [instorage_const.REP_CONSIS_SYNC,
                        instorage_const.REP_CONSIS_COPYING]):
                    if rccg['primary'] == 'master':
                        self._assistant.start_rccg(rccg_name, primary='master')
                    else:
                        self._assistant.start_rccg(rccg_name, primary='aux')
            else:
                LOG.warning('group %(grp)s is not in sync.',
                            {'grp': rccg_name})
        except exception.VolumeBackendAPIException as ex:
            LOG.warning('Fail to copy data from aux group %(rccg)s to master '
                        'group. Please recheck the relationship and '
                        'synchronize the group on backend storage. error='
                        '%(error)s', {'rccg': rccg['name'], 'error': ex})

    def _wait_replica_groups_ready(self, ctxt, groups):
        for grp in groups:
            rccg_name = self.generate_rccg_name(grp)
            self._wait_replica_grp_ready(ctxt, rccg_name)

    def _wait_replica_grp_ready(self, ctxt, rccg_name):
        LOG.debug('_wait_replica_grp_ready: group=%(rccg)s',
                {'rccg': rccg_name})

        def _replica_grp_ready():
            rccg = self._assistant.get_remotecopy_consistgrp_attr(rccg_name)
            if not rccg:
                msg = (_('_replica_grp_ready: no group %(rccg)s exists on the '
                        'backend. Please re-create the rccg and synchronize '
                        'the volumes on backend storage.'),
                        {'rccg': rccg_name})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            if rccg['relationship_count'] == '0':
                return True
            LOG.debug('_replica_grp_ready: group: %(rccg)s: state=%(state)s, '
                    'primary=%(primary)s',
                    {'rccg': rccg['name'], 'state': rccg['state'],
                    'primary': rccg['primary']})
            if rccg['state'] in [instorage_const.REP_CONSIS_SYNC]:
                return True
            if rccg['state'] == instorage_const.REP_IDL_DISC:
                msg = (_('Wait synchronize failed. group: %(rccg)s') %
                    {'rccg': rccg_name})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            return False
        try:
            self._assistant.wait_for_a_condition(
                _replica_grp_ready,
                timeout=instorage_const.DEFAULT_RC_TIMEOUT,
                interval=instorage_const.DEFAULT_RC_INTERVAL,
                raise_exception=True)
        except Exception as ex:
            msg = ('_wait_replica_grp_ready: wait for group %(rccg)s '
                    'synchronization failed due to '
                    'error: %(err)s.', {'rccg': rccg_name,
                                        'err': ex})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _replication_failover(self, ctxt, volumes, groups):
        volumes_update = []
        groups_update = []
        if self._active_backend_id:
            LOG.info(_("Host has been failed over to %s"),
                     self._active_backend_id)
            return self._active_backend_id, volumes_update, groups_update

        try:
            self._aux_backend_assistant.get_system_info()
        except Exception as ex:
            msg = (_("Unable to failover due to replication target is not "
                     "reachable. error=%(ex)s"), {'error': six.text_type(ex)})
            LOG.error(msg)
            raise exception.UnableToFailOver(reason=msg)

        normal_volumes, rep_volumes = self._classify_volume(ctxt, volumes)

        rep_volumes_update = self._failover_replica_volumes(ctxt, rep_volumes)
        volumes_update.extend(rep_volumes_update)

        rep_vols_in_grp_update, groups_update = self._failover_replica_groups(
            ctxt, groups)
        volumes_update.extend(rep_vols_in_grp_update)

        normal_volumes_update = self._failover_normal_volumes(normal_volumes)
        volumes_update.extend(normal_volumes_update)

        self._assistant = self._aux_backend_assistant
        self._active_backend_id = self._replica_target['backend_id']
        self._secondary_pools = [self._replica_target['pool_name']]

        # Update the instorage state
        self._update_instorage_state()
        self._update_volume_stats()
        return self._active_backend_id, volumes_update, groups_update

    def _failover_replica_volumes(self, ctxt, rep_volumes):
        LOG.debug('enter: _failover_replica_volumes')
        volumes_update = []

        for volume in rep_volumes:
            vol_name = self.generate_volume_name(volume)
            rep_type = self._get_volume_replicated_type(ctxt, volume)
            replica_obj = self._get_replica_obj(rep_type)
            # Try do the fail-over.
            try:
                rep_info = self._aux_backend_assistant.get_relationship_info(
                    instorage_const.REPLICA_AUX_VOL_PREFIX + vol_name)
                if not rep_info:
                    volumes_update.append(
                        {'volume_id': volume['id'],
                         'updates':
                             {'replication_status': 'failover-error',
                              'status': 'error'}})
                    LOG.error(_('_failover_replica_volumes: no rc-'
                                'releationship is established for master:'
                                '%(master)s. Please re-establish the rc-'
                                'relationship and synchronize the volumes on'
                                ' backend storage.'),
                              {'master': vol_name})
                    continue
                LOG.debug('_failover_replica_volumes: vol=%(vol)s, '
                          'master_vol=%(master_vol)s, aux_vol=%(aux_vol)s, '
                          'state=%(state)s, primary=%(primary)s',
                          {'vol': vol_name,
                           'master_vol': rep_info['master_vdisk_name'],
                           'aux_vol': rep_info['aux_vdisk_name'],
                           'state': rep_info['state'],
                           'primary': rep_info['primary']})
                if volume.status == 'in-use':
                    LOG.warning('_failover_replica_volumes: failover in-use '
                                'volume: %(volume)s is not recommended.',
                                {'volume': vol_name})
                replica_obj.failover_volume_host(ctxt, volume)
                model_updates = {
                    'replication_status': 'failed-over'}
                volumes_update.append(
                    {'volume_id': volume['id'],
                     'updates': model_updates})
            except exception.VolumeDriverException:
                LOG.error(_('Unable to failover to aux volume. Please make '
                            'sure that the aux volume is ready.'))
                volumes_update.append(
                    {'volume_id': volume['id'],
                     'updates': {'status': 'error',
                                 'replication_status': 'failed-over'}})
        LOG.debug('leave: _failover_replica_volumes '
                  'volumes_update=%(volumes_update)s',
                  {'volumes_update': volumes_update})
        return volumes_update

    def _failover_normal_volumes(self, normal_volumes):
        volumes_update = []
        for volume in normal_volumes:
            # If the volume is not of replicated type, we need to
            # force the status into error state so a user knows they
            # do not have access to the volume.
            rep_data = json.dumps({'previous_status': volume['status']})
            volumes_update.append(
                {'volume_id': volume['id'],
                 'updates': {'status': 'error',
                             'replication_driver_data': rep_data}})
        return volumes_update

    def _classify_volume(self, ctxt, volumes):
        normal_volumes = []
        replica_volumes = []

        for vol in volumes:
            volume_type = self._get_volume_replicated_type(ctxt, vol)
            grp = vol.group
            if grp and volume_utils.is_group_a_type(
                    grp, "consistent_group_replication_enabled"):
                continue
            elif volume_type and vol['status'] in ['available', 'in-use']:
                replica_volumes.append(vol)
            else:
                normal_volumes.append(vol)

        return normal_volumes, replica_volumes

    def _failover_replica_groups(self, ctxt, groups):
        volumes_update = []
        groups_update = []
        for grp in groups:
            try:
                grp_rep_status = self._rep_grp_failover(
                    ctxt, grp)['replication_status']
            except Exception as ex:
                LOG.error('Fail to failover group %(grp)s during host '
                          'failover due to error: %(error)s',
                          {'grp': grp.id, 'error': ex})
                grp_rep_status = 'error'

            # Update all the volumes' status in that group
            for vol in grp.volumes:
                vol_update = {'volume_id': vol.id,
                              'updates':
                                  {'replication_status': grp_rep_status,
                                   'status': (
                                       vol.status if grp_rep_status ==
                                       'failed-over'
                                       else 'error')}}
                volumes_update.append(vol_update)
            grp_status = ('available' if grp_rep_status == 'failed-over'
                          else 'error')
            grp_update = {'group_id': grp.id,
                          'updates': {'replication_status': grp_rep_status,
                                      'status': grp_status}}
            groups_update.append(grp_update)
        return volumes_update, groups_update

    def _get_replica_obj(self, rep_type):
        replica_manager = self.replica_manager[
            self._replica_target['backend_id']]
        return replica_manager.get_replica_obj(rep_type)

    def _get_replica_mgr(self):
        replica_manager = self.replica_manager[
            self._replica_target['backend_id']]
        return replica_manager

    def _get_target_vol(self, volume):
        tgt_vol = self.generate_volume_name(volume)
        if self._active_backend_id:
            ctxt = context.get_admin_context()
            rep_type = self._get_volume_replicated_type(ctxt, volume)
            if rep_type:
                tgt_vol = instorage_const.REPLICA_AUX_VOL_PREFIX + tgt_vol
        return tgt_vol

    def _validate_replication_enabled(self):
        if not self._replica_enabled:
            msg = _("Replication is not properly configured on backend.")
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _get_specs_replicated_type(self, volume_type):
        replication_type = None
        extra_specs = volume_type.get("extra_specs", {})
        rep_val = extra_specs.get('replication_enabled')
        if rep_val == "<is> True":
            replication_type = extra_specs.get('replication_type',
                                               instorage_const.ASYNC)
            # The format for replication_type in extra spec is in
            # "<in> async". Otherwise, the code will
            # not reach here.
            if replication_type != instorage_const.ASYNC:
                # Pick up the replication type specified in the
                # extra spec from the format like "<in> async".
                replication_type = replication_type.split()[1]
            if replication_type not in instorage_const.VALID_REP_TYPES:
                msg = (_("Invalid replication type %s.") % replication_type)
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        return replication_type

    def _get_volume_replicated_type(self, ctxt, volume, vol_type_id=None):
        replication_type = None
        volume_type = None
        volume_type_id = volume.get("volume_type_id") if volume else vol_type_id
        if volume_type_id:
            volume_type = volume_types.get_volume_type(
                ctxt, volume_type_id)
        if volume_type:
            replication_type = self._get_specs_replicated_type(volume_type)
        return replication_type

    def _get_instorage_config(self):
        self._do_replication_setup()

        if self._active_backend_id and self._replica_target:
            self._assistant = self._aux_backend_assistant

        self._replica_enabled = (True if (self._assistant.
                                          replication_licensed() and
                                          self._replica_target) else False)
        if self._replica_enabled:
            self._supported_replica_types = instorage_const.VALID_REP_TYPES

    def _do_replication_setup(self):
        rep_devs = self.configuration.safe_get('replication_device')
        if not rep_devs:
            return

        if len(rep_devs) > 1:
            raise exception.InvalidInput(
                reason='Multiple replication devices are configured. '
                       'Now only one replication_device is supported.')

        required_flags = ['san_ip', 'backend_id', 'san_login',
                          'san_password', 'pool_name']
        for flag in required_flags:
            if flag not in rep_devs[0]:
                raise exception.InvalidInput(
                    reason=_('%s is not set.') % flag)

        rep_target = {}
        rep_target['san_ip'] = rep_devs[0].get('san_ip')
        rep_target['backend_id'] = rep_devs[0].get('backend_id')
        rep_target['san_login'] = rep_devs[0].get('san_login')
        rep_target['san_password'] = rep_devs[0].get('san_password')
        rep_target['pool_name'] = rep_devs[0].get('pool_name')
        rep_target['ssh_conn_timeout'] = self.configuration.ssh_conn_timeout
        rep_target['ssh_min_pool_conn'] = self.configuration.ssh_min_pool_conn
        rep_target['ssh_max_pool_conn'] = self.configuration.ssh_max_pool_conn
        rep_target['ssh_exec_timeout'] = self.configuration.ssh_exec_timeout
        if ((self.configuration.instorage_rsa_private_key is not None) and
                (self.configuration.instorage_rsa_private_key.strip() != '')):
            if not self.rsa_util:
                self.rsa_util = RsaUtil(self.configuration.instorage_rsa_private_key)
            rep_target['san_password'] = self.rsa_util.decrypt_by_private_key(
                rep_devs[0].get('san_password'))

        # Each replication target will have a corresponding replication.
        self._replication_initialize(rep_target)

    def _replication_initialize(self, target):
        rep_manager = instorage_rep.InStorageMCSReplicationManager(
            self, target, instorage_cli.InStorageAssistant)

        if self._active_backend_id:
            if self._active_backend_id != target['backend_id']:
                msg = (_("Invalid secondary id %s.") % self._active_backend_id)
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)
        # Setup partnership only in non-failover state
        else:
            try:
                rep_manager.establish_target_partnership()
            except exception.VolumeDriverException:
                LOG.error(_('The replication src %(src)s has not '
                            'successfully established partnership with the '
                            'replica target %(tgt)s.'),
                          {'src': self.configuration.san_ip,
                           'tgt': target['backend_id']})

        self._aux_backend_assistant = rep_manager.get_target_assistant()
        self.replica_manager[target['backend_id']] = rep_manager
        self._replica_target = target
