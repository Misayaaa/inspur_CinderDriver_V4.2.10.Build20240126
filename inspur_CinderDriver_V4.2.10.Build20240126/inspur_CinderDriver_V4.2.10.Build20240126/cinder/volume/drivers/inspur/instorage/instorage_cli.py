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
CLI for inspur Instorage
"""

import random
import re
import time
import unicodedata
from eventlet import greenthread
import six

from cinder import context
from cinder import exception
from cinder.i18n import _
from cinder import utils as cinder_utils
from cinder.volume import volume_types

from cinder.volume.drivers.inspur import constants as instorage_const
from cinder.volume import qos_specs
from cinder.volume.drivers.inspur import utils as as18000_utils


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

INTERVAL_1_SEC = instorage_const.INTERVAL_1_SEC
DEFAULT_TIMEOUT = instorage_const.DEFAULT_TIMEOUT
log_filter = as18000_utils.LogFilter
utils_trace = as18000_utils.inspur_trace

LOG = logging.getLogger(__name__)


class InStorageExector(object):
    """Exector interface to inspur InStorage systems."""

    def __init__(self, run_ssh):
        self._ssh = run_ssh

    def _run_ssh(self, ssh_cmd):
        try:
            return self._ssh(ssh_cmd)
        except processutils.ProcessExecutionError as exp:
            msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                     'stdout: %(out)s\n stderr: %(err)s.') %
                   {'cmd': ssh_cmd,
                    'out': exp.stdout,
                    'err': exp.stderr})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)


    def run_ssh_inq(self, ssh_cmd, delim='!', with_header=False):
        """Run an SSH command and return parsed output."""
        raw = self._run_ssh(ssh_cmd)
        return CLIParser(raw, ssh_cmd=ssh_cmd, delim=delim,
                         with_header=with_header)

    def run_ssh_assert_no_output(self, ssh_cmd):
        """Run an SSH command and assert no output returned."""
        out, _ = self._run_ssh(ssh_cmd)
        if len(out.strip()) != 0:
            msg = (_('Expected no output from CLI command %(cmd)s, '
                     'got %(out)s.') % {'cmd': ' '.join(ssh_cmd), 'out': out})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def run_ssh_check_created(self, ssh_cmd):
        """Run an SSH command and return the ID of the created object."""
        out, err = self._run_ssh(ssh_cmd)
        try:
            match_obj = re.search(r'\[([0-9]+)\],? successfully created', out)
            return match_obj.group(1)
        except (AttributeError, IndexError):
            msg = (_('Failed to parse CLI output:\n command: %(cmd)s\n '
                     'stdout: %(out)s\n stderr: %(err)s.') %
                   {'cmd': ssh_cmd,
                    'out': out,
                    'err': err})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def lsnode(self, node_id=None):
        with_header = True
        ssh_cmd = ['mcsinq', 'lsnode', '-delim', '!']
        if node_id:
            with_header = False
            ssh_cmd.append(node_id)
        return self.run_ssh_inq(ssh_cmd, with_header=with_header)

    def retry_func(func, attempts=3):
        def wrapper(self, *args, **kw):
            for retry_num in range(attempts):
                try:
                    return func(self, *args, **kw)
                except IndexError:
                    if retry_num == attempts - 1:
                        LOG.error('%(func)s, Retry was exceeded. Unable '
                                'to execute relevant command',
                                {'func': func.__name__})
                        raise
                    LOG.warning('%(func)s, retry, '
                                'TryNum: %(rn)s.',
                                {'func': func.__name__,
                                'rn': retry_num + 1})
                    greenthread.sleep(5)
        return wrapper

    @retry_func
    def lslicense(self):
        ssh_cmd = ['mcsinq', 'lslicense', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd)[0]

    @retry_func
    def lsguicapabilities(self):
        ssh_cmd = ['mcsinq', 'lsguicapabilities', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd)[0]

    @retry_func
    def lssystem(self):
        ssh_cmd = ['mcsinq', 'lssystem', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd)[0]

    @retry_func
    def lssystemcapcities(self):
        ssh_cmd = ['mcsinq', 'lssystem', '-bytes', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd)[0]

    @retry_func
    def lsmdiskgrp(self, pool):
        ssh_cmd = ['mcsinq', 'lsmdiskgrp', '-bytes', '-delim', '!',
                   '"%s"' % pool]
        return self.run_ssh_inq(ssh_cmd)[0]

    def lsiogrp(self):
        ssh_cmd = ['mcsinq', 'lsiogrp', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lsportip(self):
        ssh_cmd = ['mcsinq', 'lsportip', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lshost(self, host=None):
        with_header = True
        ssh_cmd = ['mcsinq', 'lshost', '-delim', '!']
        if host:
            with_header = False
            ssh_cmd.append('"%s"' % host)
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err:
            return CLIParser((out, err), ssh_cmd=ssh_cmd, delim='!',
                             with_header=with_header)
        if 'CMMVC5754E' in err:
            return None
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)
        
    def lsiscsiauth(self):
        ssh_cmd = ['mcsinq', 'lsiscsiauth', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lsfabric(self, wwpn=None, host=None):
        ssh_cmd = ['mcsinq', 'lsfabric', '-delim', '!']
        if wwpn:
            ssh_cmd.extend(['-wwpn', wwpn])
        elif host:
            ssh_cmd.extend(['-host', '"%s"' % host])
        else:
            msg = (_('Must pass wwpn or host to lsfabric.'))
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lsrcrelationship(self, key, rc_rel):
        key_value = '%s=%s' % (key, rc_rel)
        ssh_cmd = ['mcsinq', 'lsrcrelationship', '-filtervalue',
                   key_value, '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lspartnership(self, system_name):
        key_value = 'name=%s' % system_name
        ssh_cmd = ['mcsinq', 'lspartnership', '-filtervalue',
                   key_value, '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lspartnershipcandidate(self):
        ssh_cmd = ['mcsinq', 'lspartnershipcandidate', '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lsvdiskhostmap(self, vdisk):
        ssh_cmd = ['mcsinq', 'lsvdiskhostmap', '-delim', '!', '"%s"' % vdisk]
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err:
            return CLIParser((out, err), ssh_cmd=ssh_cmd, delim='!',
                             with_header=True)
        if 'CMMVC5754E' in err:
            return []
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def lshostvdiskmap(self, host):
        ssh_cmd = ['mcsinq', 'lshostvdiskmap', '-delim', '!', '"%s"' % host]
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    @retry_func
    @utils_trace
    @log_filter.register_function()
    def lsvdisk(self, vdisk):
        """Return vdisk attributes or None if it doesn't exist."""
        ssh_cmd = ['mcsinq', 'lsvdisk', '-bytes', '-delim', '!',
                   '"%s"' % vdisk]
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err:
            return CLIParser((out, err), ssh_cmd=ssh_cmd, delim='!',
                             with_header=False)[0]
        if 'CMMVC5754E' in err:
            return None
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def lsvdisks_from_filter(self, filter_name, value):
        """Performs an lsvdisk command, filtering the results as specified.

        Returns an iterable for all matching vdisks.
        """
        ssh_cmd = ['mcsinq', 'lsvdisk', '-bytes', '-delim', '!',
                   '-filtervalue', '%s=%s' % (filter_name, value)]
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lsvdisklcmappings(self, vdisk):
        ssh_cmd = ['mcsinq', 'lsvdisklcmappings', '-delim', '!',
                   '"%s"' % vdisk]
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lslcmap(self, lc_map_id):
        ssh_cmd = ['mcsinq', 'lslcmap', '-gui', '-filtervalue',
                   'id=%s' % lc_map_id, '-delim', '!']
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lslcconsistgrp(self, lc_consistgrp):
        ssh_cmd = ['mcsinq', 'lslcconsistgrp', '-delim', '!', lc_consistgrp]
        out, err = self._ssh(ssh_cmd)
        return CLIParser((out, err), ssh_cmd=ssh_cmd, delim='!',
                         with_header=False)

    def lsrcconsistgrp(self, rc_consistgrp):
        ssh_cmd = ['mcsinq', 'lsrcconsistgrp', '-delim', '!', rc_consistgrp]
        out, err = self._ssh(ssh_cmd)
        return CLIParser((out, err), ssh_cmd=ssh_cmd, delim='!',
                         with_header=False)

    def lsvdiskcopy(self, vdisk=None, copy_id=None, pool=None):
        ssh_cmd = ['mcsinq', 'lsvdiskcopy', '-gui', '-bytes', '-delim', '!']
        with_header = True
        if copy_id:
            ssh_cmd += ['-copy', copy_id]
            with_header = False
        if vdisk:
            ssh_cmd += ['"%s"' % vdisk]
        if pool:
            ssh_cmd += ['-filtervalue', 'mdisk_grp_name=%s' % pool]
        return self.run_ssh_inq(ssh_cmd, with_header=with_header)

    @retry_func
    def lsvdisksyncprogress(self, vdisk, copy_id):
        ssh_cmd = ['mcsinq', 'lsvdisksyncprogress', '-delim', '!',
                   '-copy', copy_id, '"%s"' % vdisk]
        return self.run_ssh_inq(ssh_cmd, with_header=True)[0]

    def lsportfc(self, node_id):
        ssh_cmd = ['mcsinq', 'lsportfc', '-delim', '!',
                   '-filtervalue', 'node_id=%s' % node_id]
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    def lsrmvdiskdependentmaps(self, vdisk):
        ssh_cmd = ['mcsinq', 'lsrmvdiskdependentmaps', '-delim', '!', '"%s"' % vdisk]
        return self.run_ssh_inq(ssh_cmd, with_header=True)

    @staticmethod
    def _create_port_arg(port_type, port_name):
        if port_type == 'initiator':
            port = ['-iscsiname']
        elif port_type == 'wwpn':
            port = ['-hbawwpn']
        else:
            port = ['-nqn']
        port.append(port_name)
        return port

    def mkhost(self, host_name, port_type, port_name, site='', protocol = 'scsi'):
        port = self._create_port_arg(port_type, port_name)
        ssh_cmd = ['mcsop', 'mkhost', '-force'] + port
        ssh_cmd += ['-name', '"%s"' % host_name]
        if site:
            ssh_cmd += ['-site', '"%s"' % site]
        if protocol == 'nvme':
            ssh_cmd += ['-protocol', 'nvme']
            ssh_cmd += ['-nvmetransport', 'roce']
        return self.run_ssh_check_created(ssh_cmd)

    def addhostport(self, host, port_type, port_name):
        port = self._create_port_arg(port_type, port_name)
        ssh_cmd = ['mcsop', 'addhostport', '-force'] + port + ['"%s"' % host]
        self.run_ssh_assert_no_output(ssh_cmd)

    def add_chap_secret(self, secret, host):
        ssh_cmd = ['mcsop', 'chhost', '-chapsecret', secret, '"%s"' % host]
        self.run_ssh_assert_no_output(ssh_cmd)

    def mkvdiskhostmap(self, host, vdisk, lun, multihostmap, protocol = 'scsi'):
        """Map vdisk to host.

        If vdisk already mapped and multihostmap is True, use the force flag.
        """
        ssh_cmd = ['mcsop', 'mkvdiskhostmap', '-host', '"%s"' % host, vdisk]

        if lun:
            ssh_cmd.insert(ssh_cmd.index(vdisk), '-scsi')
            ssh_cmd.insert(ssh_cmd.index(vdisk), lun)

        if multihostmap:
            ssh_cmd.insert(ssh_cmd.index('mkvdiskhostmap') + 1, '-force')
        try:
            self.run_ssh_check_created(ssh_cmd)
            if protocol == 'scsi':
                iogrp_lun_id_map = self.get_vdiskhostmapid(vdisk, host)
                if len(iogrp_lun_id_map) == 0:
                    msg = (_('mkvdiskhostmap error:\n command: %(cmd)s\n '
                            'lun: %(lun)s\n iogrp_lun_id_map: %(iogrp_lun_id_map)s') %
                        {'cmd': ssh_cmd,
                            'lun': lun,
                            'iogrp_lun_id_map': iogrp_lun_id_map})
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)
                return iogrp_lun_id_map
            else:
                is_mapped = self.check_volume_mapped(vdisk, host)
                if not is_mapped:
                    msg = (_('mkvdiskhostmap error:\n command: %(cmd)s\n '
                            'lun: %(lun)s\n ') %
                        {'cmd': ssh_cmd,
                            'lun': lun})
                    LOG.error(msg)
                    raise exception.VolumeDriverException(message=msg)
                return True
        except Exception as ex:
            if (not multihostmap and hasattr(ex, 'message') and
                    'CMMVC6071E' in ex.message):
                LOG.error(_('instorage_mcs_multihostmap_enabled is set '
                            'to False, not allowing multi host mapping.'))
                raise exception.VolumeDriverException(
                    message=_('CMMVC6071E The VDisk-to-host mapping was not '
                              'created because the VDisk is already mapped '
                              'to a host.\n"'))
            with excutils.save_and_reraise_exception():
                LOG.error(_('Error mapping VDisk-to-host'))

    def mkrcrelationship(self, master, aux, system, asynccopy):
        ssh_cmd = ['mcsop', 'mkrcrelationship', '-master', master,
                   '-aux', aux, '-cluster', system]
        if asynccopy:
            ssh_cmd.append('-async')
        return self.run_ssh_check_created(ssh_cmd)

    def rmrcrelationship(self, relationship, force=False):
        ssh_cmd = ['mcsop', 'rmrcrelationship']
        if force:
            ssh_cmd += ['-force']
        ssh_cmd += [relationship]
        self.run_ssh_assert_no_output(ssh_cmd)

    def switchrelationship(self, relationship, aux=True):
        primary = 'aux' if aux else 'master'
        ssh_cmd = ['mcsop', 'switchrcrelationship', '-primary',
                   primary, relationship]
        self.run_ssh_assert_no_output(ssh_cmd)

    def startrcrelationship(self, rc_rel, primary=None):
        ssh_cmd = ['mcsop', 'startrcrelationship', '-force']
        if primary:
            ssh_cmd.extend(['-primary', primary])
        ssh_cmd.append(rc_rel)
        self.run_ssh_assert_no_output(ssh_cmd)

    def startrcconsistgrp(self, rc_consistgrp, primary=None):
        ssh_cmd = ['mcsop', 'startrcconsistgrp', '-force']
        if primary:
            ssh_cmd.extend(['-primary', primary])
        ssh_cmd.append(rc_consistgrp)
        self.run_ssh_assert_no_output(ssh_cmd)

    def stoprcconsistgrp(self, rccg, access=False):
        ssh_cmd = ['mcsop', 'stoprcconsistgrp']
        if access:
            ssh_cmd.append('-access')
        ssh_cmd.append(rccg)
        self.run_ssh_assert_no_output(ssh_cmd)

    def stoprcrelationship(self, relationship, access=False):
        ssh_cmd = ['mcsop', 'stoprcrelationship']
        if access:
            ssh_cmd.append('-access')
        ssh_cmd.append(relationship)
        self.run_ssh_assert_no_output(ssh_cmd)

    def mkippartnership(self, ip_v4, bandwith=1000, backgroundcopyrate=50):
        ssh_cmd = ['mcsop', 'mkippartnership', '-type', 'ipv4',
                   '-clusterip', ip_v4, '-linkbandwidthmbits',
                   six.text_type(bandwith),
                   '-backgroundcopyrate', six.text_type(backgroundcopyrate)]
        return self.run_ssh_assert_no_output(ssh_cmd)

    def mkfcpartnership(self, system_name, bandwith=1000,
                        backgroundcopyrate=50):
        ssh_cmd = ['mcsop', 'mkfcpartnership', '-linkbandwidthmbits',
                   six.text_type(bandwith),
                   '-backgroundcopyrate', six.text_type(backgroundcopyrate),
                   system_name]
        return self.run_ssh_assert_no_output(ssh_cmd)

    def chpartnership(self, partnership_id, start=True):
        action = '-start' if start else '-stop'
        ssh_cmd = ['mcsop', 'chpartnership', action, partnership_id]
        return self.run_ssh_assert_no_output(ssh_cmd)

    def rmvdiskhostmap(self, host, vdisk):
        ssh_cmd = ['mcsop', 'rmvdiskhostmap', '-host', '"%s"' % host,
                   '"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def get_vdiskhostmapid(self, vdisk, host):
        # return the map that the key is io_group_id and the value is lun_id
        result_map = {}
        resp = self.lsvdiskhostmap(vdisk)
        for mapping_info in resp:
            if mapping_info['host_name'] == host:
                result_map[mapping_info['IO_group_id']] = int(mapping_info['SCSI_id'])
        return result_map

    def check_volume_mapped(self, vdisk, host):
        resp = self.lsvdiskhostmap(vdisk)
        for mapping_info in resp:
            if mapping_info['host_name'] == host:
                return True
        return False

    def rmhost(self, host):
        ssh_cmd = ['mcsop', 'rmhost', '"%s"' % host]
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err and not out:
            return
        if 'CMMVC5753E' in err:
            # CMMVC5753E host not exist.
            return None
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def mkvolume(self, name, size, units, poollist, iogrplist, params):
        ssh_cmd = ['mcsop', 'mkvolume', '-name', name,
                   '-size', size, '-unit', units,
                   '-pool', poollist] + params

        try:
            return self.run_ssh_check_created(ssh_cmd)
        except Exception as ex:
            if hasattr(ex, 'msg') and 'CMMVC6035E' in ex.msg:
                LOG.warning(_('CMMVC6035E The volume already exists '))
                return
            with excutils.save_and_reraise_exception():
                LOG.exception(_('Failed to create vdisk %(vol)s.'),
                              {'vol': name})

    @utils_trace
    @log_filter.register_function()
    def mkvdisk(self, name, size, units, pool, opts, params, nofmtdisk=False):
        ssh_cmd = ['mcsop', 'mkvdisk', '-name', name, '-mdiskgrp',
                   '"%s"' % pool, '-iogrp', six.text_type(opts['iogrp']),
                   '-size', size, '-unit', units] + params
        if nofmtdisk:
            ssh_cmd.extend(['-nofmtdisk'])
        if opts['node']:
            ssh_cmd.extend(['-node','%s' % opts['node']])
        if opts['access_iogrp']:
            ssh_cmd.extend(['-accessiogrp', '%s' % opts['access_iogrp']])
        try:
            return self.run_ssh_check_created(ssh_cmd)
        except Exception as ex:
            if hasattr(ex, 'msg') and 'CMMVC6372W' in ex.msg:
                vdisk = self.lsvdisk(name)
                if vdisk:
                    LOG.warning(_('CMMVC6372W The virtualized storage '
                                  'capacity that the cluster is using is '
                                  'approaching the virtualized storage '
                                  'capacity that is licensed.'))
                    return vdisk['id']
            elif hasattr(ex, 'msg') and 'CMMVC6035E' in ex.msg:
                LOG.warning(_('CMMVC6035E The volume already exists '))
                return
            with excutils.save_and_reraise_exception():
                LOG.exception(_('Failed to create vdisk %(vol)s.'),
                              {'vol': name})

    def rmvolume(self, vdisk, force=True):
        ssh_cmd = ['mcsop', 'rmvolume']
        if force:
            ssh_cmd += ['-removehostmappings', '-removercrelationships',
                        '-removelcmaps', '-discardimage', '-cancelbackup']
        ssh_cmd += ['"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def rmvdisk(self, vdisk, force=True):
        ssh_cmd = ['mcsop', 'rmvdisk']
        if force:
            ssh_cmd += ['-force']
        ssh_cmd += ['"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def chvdisk(self, vdisk, params):
        ssh_cmd = ['mcsop', 'chvdisk'] + params + ['"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def movevdisk(self, vdisk, iogrp):
        ssh_cmd = ['mcsop', 'movevdisk', '-iogrp', iogrp, '"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def expandvdisksize(self, vdisk, amount):
        ssh_cmd = (
            ['mcsop', 'expandvdisksize', '-size', six.text_type(amount),
             '-unit', 'gb', '"%s"' % vdisk])
        self.run_ssh_assert_no_output(ssh_cmd)

    @utils_trace
    @log_filter.register_function()
    def mklcmap(self, source, target, full_copy, copy_rate, clean_rate, consistgrp=None):
        ssh_cmd = ['mcsop', 'mklcmap', '-source', '"%s"' % source, '-target',
                   '"%s"' % target]
        if not full_copy:
            ssh_cmd.extend(['-copyrate', '0'])
        else:
            ssh_cmd.extend(['-copyrate', six.text_type(copy_rate)])
            ssh_cmd.extend(['-cleanrate', six.text_type(clean_rate)])
            ssh_cmd.extend(['-autodelete'])
        if consistgrp:
            ssh_cmd.extend(['-consistgrp', consistgrp])
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if 'successfully created' not in out:
            msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                     'stdout: %(out)s\n stderr: %(err)s.') %
                   {'cmd': ssh_cmd,
                    'out': out,
                    'err': err})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        try:
            match_obj = re.search(r'LocalCopy Mapping, id \[([0-9]+)\], '
                                  'successfully created', out)
            lc_map_id = match_obj.group(1)
        except (AttributeError, IndexError):
            msg = (_('Failed to parse CLI output:\n command: %(cmd)s\n '
                     'stdout: %(out)s\n stderr: %(err)s.') %
                   {'cmd': ssh_cmd,
                    'out': out,
                    'err': err})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return lc_map_id

    @utils_trace
    @log_filter.register_function()
    def prestartlcmap(self, lc_map_id, restore=False):
        ssh_cmd = ['mcsop', 'prestartlcmap']
        if restore:
            ssh_cmd.append('-restore')
        ssh_cmd.append(lc_map_id)
        self.run_ssh_assert_no_output(ssh_cmd)

    @utils_trace
    @log_filter.register_function()
    def startlcmap(self, lc_map_id, restore=False):
        ssh_cmd = ['mcsop', 'startlcmap', '-prep']
        if restore:
            ssh_cmd.append('-restore')
        ssh_cmd.append(lc_map_id)
        self.run_ssh_assert_no_output(ssh_cmd)

    def prestartlcconsistgrp(self, lc_consist_group):
        ssh_cmd = ['mcsop', 'prestartlcconsistgrp', lc_consist_group]
        self.run_ssh_assert_no_output(ssh_cmd)

    def startlcconsistgrp(self, lc_consist_group):
        ssh_cmd = ['mcsop', 'startlcconsistgrp', lc_consist_group]
        self.run_ssh_assert_no_output(ssh_cmd)

    def stoplcconsistgrp(self, lc_consist_group):
        ssh_cmd = ['mcsop', 'stoplcconsistgrp', lc_consist_group]
        self.run_ssh_assert_no_output(ssh_cmd)

    def chlcmap(self, lc_map_id, copyrate='50', autodel='on'):
        ssh_cmd = ['mcsop', 'chlcmap', '-copyrate', copyrate,
                   '-autodelete', autodel, lc_map_id]
        self.run_ssh_assert_no_output(ssh_cmd)

    def stoplcmap(self, lc_map_id, split=False):
        ssh_cmd = ['mcsop', 'stoplcmap']
        if split:
            ssh_cmd += ['-split']
        ssh_cmd += ['"%s"' % lc_map_id]
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err and not out:
            return
        LOG.warning('Operation stoplcmap exec failed for %(out)s %(err)s' %
                    {'out': out, 'err': err})
        if 'CMMVC5753E' in err:
            # CMMVC5753E lcmap not exist.
            return None
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    @cinder_utils.retry(exception.VolumeBackendAPIException, interval=5, retries=60, backoff_rate=1)
    def rmlcmap(self, lc_map_id):
        ssh_cmd = ['mcsop', 'rmlcmap', '-force', lc_map_id]
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err and not out:
            return
        LOG.warning('Operation rmlcmap exec failed for %(out)s %(err)s' %
                    {'out': out, 'err': err})
        if 'CMMVC5753E' in err:
            # CMMVC5753E lcmap not exist.
            return None
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def mklcconsistgrp(self, lc_consist_group, fast=False):
        ssh_cmd = ['mcsop', 'mklcconsistgrp', '-name', lc_consist_group]
        if fast:
            ssh_cmd += ['-fast']
        return self.run_ssh_check_created(ssh_cmd)

    def rmlcconsistgrp(self, lc_consist_group):
        ssh_cmd = ['mcsop', 'rmlcconsistgrp', '-force', lc_consist_group]
        return self.run_ssh_assert_no_output(ssh_cmd)

    def mkrcconsistgrp(self, group_name, system_id):
        ssh_cmd = ['mcsop', 'mkrcconsistgrp', '-name', group_name,
                   '-cluster', system_id]
        return self.run_ssh_check_created(ssh_cmd)

    def rmrcconsistgrp(self, group_name):
        ssh_cmd = ['mcsop', 'rmrcconsistgrp', '-force', group_name]
        out, err = self._ssh(ssh_cmd, check_exit_code=False)
        if not err and not out:
            return
        LOG.warning('Operation rmrcconsistgrp exec failed for %(out)s %(err)s' %
                    {'out': out, 'err': err})
        if 'CMMVC5753E' in err:
            # CMMVC5753E lcmap not exist.
            return None
        msg = (_('CLI Exception output:\n command: %(cmd)s\n '
                 'stdout: %(out)s\n stderr: %(err)s.') %
               {'cmd': ssh_cmd,
                'out': out,
                'err': err})
        LOG.error(msg)
        raise exception.VolumeBackendAPIException(data=msg)

    def chrcrelationship(self, rcrel_name, rccg_name):
        ssh_cmd = ['mcsop', 'chrcrelationship']
        if rccg_name:
            ssh_cmd.extend(['-consistgrp', rccg_name])
        else:
            ssh_cmd.extend(['-noconsistgrp'])
        ssh_cmd.append(rcrel_name)
        return self.run_ssh_assert_no_output(ssh_cmd)

    def addvdiskcopy(self, vdisk, dest_pool, params):
        ssh_cmd = (['mcsop', 'addvdiskcopy'] +
                   params +
                   ['-mdiskgrp', '"%s"' %
                    dest_pool, '"%s"' %
                    vdisk])
        return self.run_ssh_check_created(ssh_cmd)

    def rmvdiskcopy(self, vdisk, copy_id):
        ssh_cmd = ['mcsop', 'rmvdiskcopy', '-copy', copy_id, '"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def addvolumecopy(self, vdisk_name, dest_pool, dest_iogrp, params):
        ssh_cmd = (
            ['mcsop', 'addvolumecopy'] +
            params +
            ['-iogrp', '"%s"' % dest_iogrp] +
            ['-pool', '"%s"' % dest_pool] +
            [vdisk_name]
        )
        self.run_ssh_assert_no_output(ssh_cmd)

    def rmvolumecopy(self, name, pool):
        ssh_cmd = ['mcsop', 'rmvolumecopy', '-pool', '"%s"' % pool, name]
        self.run_ssh_assert_no_output(ssh_cmd)

    def addvdiskaccess(self, vdisk, iogrp):
        ssh_cmd = ['mcsop', 'addvdiskaccess', '-iogrp', iogrp,
                   '"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def rmvdiskaccess(self, vdisk, iogrp):
        ssh_cmd = ['mcsop', 'rmvdiskaccess', '-iogrp', iogrp, '"%s"' % vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def migratevdisk(self, vdisk, dest_pool, copy_id='0'):
        ssh_cmd = ['mcsop', 'migratevdisk', '-mdiskgrp', dest_pool, '-copy',
                   copy_id, '-vdisk', vdisk]
        self.run_ssh_assert_no_output(ssh_cmd)

    def expandremotecopysize(self, size, site, rc_name):
        ssh_cmd = ['mcsop', 'expandremotecopysize', '-size', six.text_type(size),
                   '-unit', 'gb', '-site', site]
        ssh_cmd.append(rc_name)
        self.run_ssh_assert_no_output(ssh_cmd)


class CLIParser(object):
    """Parse MCS CLI output and generate iterable."""

    def __init__(self, raw, ssh_cmd=None, delim='!', with_header=True):
        super(CLIParser, self).__init__()
        if ssh_cmd:
            self.ssh_cmd = ' '.join(ssh_cmd)
        else:
            self.ssh_cmd = 'None'
        self.raw = raw
        self.delim = delim
        self.with_header = with_header
        self.result = self._parse()

    def select(self, *keys):
        for d_item in self.result:
            vs_list = []
            for k in keys:
                value = d_item.get(k, None)
                if isinstance(value, six.string_types) or value is None:
                    value = [value]
                if isinstance(value, list):
                    vs_list.append(value)
            for item in zip(*vs_list):
                if len(item) == 1:
                    yield item[0]
                else:
                    yield item

    def __getitem__(self, key):
        try:
            return self.result[key]
        except KeyError:
            msg = (_('Did not find the expected key %(key)s in %(fun)s: '
                     '%(raw)s.') % {'key': key, 'fun': self.ssh_cmd,
                                    'raw': self.raw})
            raise exception.VolumeBackendAPIException(data=msg)

    def __iter__(self):
        for item in self.result:
            yield item

    def __len__(self):
        return len(self.result)

    def _parse(self):
        def get_reader(content, delim):
            for line in content.lstrip().splitlines():
                line = line.strip()
                if line:
                    yield line.split(delim)
                else:
                    yield []

        if isinstance(self.raw, six.string_types):
            stdout, stderr = self.raw, ''
        else:
            stdout, stderr = self.raw

        reader = get_reader(stdout, self.delim)
        result = []

        if self.with_header:
            hds = tuple()
            # add temp by zby at 12/31
            remove_flag = False
            for row in reader:
                # add temp by zby  at 12/31
                if 'host_cluster_name' in row:
                    row.remove('host_cluster_name')
                    remove_flag = True
                hds = row
                break
            for row in reader:
                cur = dict()
                # add temp by zby  at 12/31
                if remove_flag:
                    row = row[:len(hds)]
                if len(hds) != len(row):
                    err_msg = (_('stdout: %(stdout)s')
                           % {'stdout': stdout})
                    LOG.error(err_msg)
                    msg = (_('Unexpected CLI response: header/row mismatch. '
                             'header: %(header)s, row: %(row)s.')
                           % {'header': hds,
                              'row': row})
                    raise exception.VolumeBackendAPIException(data=msg)
                for key, value in zip(hds, row):
                    CLIParser.append_dict(cur, key, value)
                result.append(cur)
        else:
            cur = dict()
            for row in reader:
                if row:
                    CLIParser.append_dict(cur, row[0], ' '.join(row[1:]))
                elif cur:  # start new section
                    result.append(cur)
                    cur = dict()
            if cur:
                result.append(cur)
        return result

    @staticmethod
    def append_dict(dict_, key, value):
        key, value = key.strip(), value.strip()
        obj = dict_.get(key, None)
        if obj is None:
            dict_[key] = value
        elif isinstance(obj, list):
            obj.append(value)
            dict_[key] = obj
        else:
            dict_[key] = [obj, value]
        return dict_


class InStorageAssistant(object):
    """inspur InStorage MCS CLI assistant for cinder driver."""
    # All the supported QoS key are saved in this dict. When a new
    # key is going to add, three values MUST be set:
    # 'default': to indicate the value, when the parameter is disabled.
    # 'param': to indicate the corresponding parameter in the command.
    # 'type': to indicate the type of this value.
    WAIT_TIME = 5
    mcs_qos_keys = {'IOThrottling': {'default': '0',
                                     'param': 'rate',
                                     'type': int},
                    'Bandwidth': {'default': '0',
                                  'param': 'rate',
                                  'type': int}}

    def __init__(self, run_cmd,
                 pool_map=None, iogrp_map=None,
                 loop_check_interval=10):

        self.executor = InStorageExector(run_cmd)
        self.loop_check_interval = loop_check_interval
        self.pool_map = pool_map
        self.iogrp_map = iogrp_map

    @staticmethod
    def handle_keyerror(cmd, out):
        msg = (_('Could not find key in output of command %(cmd)s: %(out)s.')
               % {'out': out, 'cmd': cmd})
        raise exception.VolumeBackendAPIException(data=msg)

    def compression_enabled(self):
        """Return whether or not compression is enabled for this system."""
        resp = self.executor.lslicense()
        keys = ['license_compression_enclosures',
                'license_compression_capacity']
        for key in keys:
            if resp.get(key, '0') != '0':
                return True
        try:
            resp = self.executor.lsguicapabilities()
            if resp.get('compression', '0') == 'yes':
                return True
        except exception.VolumeBackendAPIException:
            LOG.exception(_("Failed to fetch licensing scheme."))
        return False

    def deduped_enabled(self):
        """Return whether or not deduplication is enabled for this system."""
        try:
            resp = self.executor.lsguicapabilities()
            if resp.get('deduplication', '0') == 'yes':
                return True
        except exception.VolumeBackendAPIException:
            LOG.exception(_("Failed to fetch deduped licensing scheme."))
        return False

    def extend_cloning_support(self, capabilities_value):
        if int(capabilities_value) & 0x01 == 1:
            LOG.info("extend a cloning volume is supported")
            return True
        LOG.info("extend a cloning volume is not supported")
        return False

    def fastconsisgrp_support(self, capabilities_value):
        if int(capabilities_value) >>1 & 0x01 == 1:
            LOG.info("fast localcopy consisgrp is supported")
            return True
        LOG.info("fast localcopy consisgrp is not supported")
        return False

    def lcmap_remotecopy_extend_support(self, capabilities_value):
        if int(capabilities_value) >>2 & 0x01 == 1:
            LOG.info("extend a volume in lcmap is supported")
            LOG.info("extend Active-Active volume online is supported")
            LOG.info("extend remotecopy volume is supported")
            return True
        LOG.info("extend a volume in lcmap is not supported")
        LOG.info("extend Active-Active volume online is not supported")
        LOG.info("extend remotecopy volume is not supported")
        return False

    def get_capabilities_value(self):
        capabilities_value = 0
        try:
            resp = self.executor.lsguicapabilities()
            capabilities_value = resp.get('license_type_localcopy', '0')
        except exception.VolumeBackendAPIException:
            LOG.exception(_("Failed to fetch license_type_localcopy."))
        return capabilities_value

    def replication_licensed(self):
        """Return whether or not replication is enabled for this system."""
        return True

    def get_system_info(self):
        """Return system's name, ID, and code level."""
        resp = self.executor.lssystem()
        level = resp['code_level']
        match_obj = re.search('([0-9]+.){3}[0-9]+', level)
        if match_obj is None:
            msg = _('Failed to get code level (%s).') % level
            raise exception.VolumeBackendAPIException(data=msg)
        code_level = match_obj.group().split('.')
        return {'code_level': tuple([int(x) for x in code_level]),
                'system_name': resp['name'],
                'system_id': resp['id'],
                'topology': resp['topology'],
                'subnqn': resp.get('subnqn', None)}

    def get_cluster_capacity_info(self):
        """Return system's total and allocated_extent capacity."""
        resp = self.executor.lssystemcapcities()
        return {'total_mdisk_capacity': resp['total_mdisk_capacity'],
                'total_allocated_extent_capacity': resp['total_allocated_extent_capacity'],
                'total_vdiskcopy_capacity': resp['total_vdiskcopy_capacity']}

    def get_node_info(self):
        """Return dictionary containing information on system's nodes."""
        nodes = {}
        resp = self.executor.lsnode()
        for node_data in resp:
            try:
                if node_data['status'] != 'online':
                    continue
                node = {}
                node['id'] = node_data['id']
                node['name'] = node_data['name']
                node['IO_group'] = node_data['IO_group_id']
                node['iscsi_name'] = node_data['iscsi_name']
                node['site_name'] = node_data['site_name']
                node['WWNN'] = node_data['WWNN']
                node['status'] = node_data['status']
                node['WWPN'] = []
                node['ipv4'] = []
                node['ipv6'] = []
                node['enabled_protocols'] = []
                nodes[node['id']] = node
            except KeyError:
                self.handle_keyerror('lsnode', node_data)
        return nodes

    def get_storage_nqn(self):
        return self.get_system_info()['subnqn']

    def get_pool_attrs(self, pool):
        """Return attributes for the specified pool."""
        return self.executor.lsmdiskgrp(pool)

    def is_ssa_pool(self, pool):
        attrs = self.get_pool_attrs(pool)
        try:
            return True if attrs['category'] == 'ssa' else False
        except (AttributeError,KeyError):
            return False

    def get_available_io_groups(self):
        """Return list of available IO groups."""
        iogrps = []
        resp = self.executor.lsiogrp()
        for iogrp in resp:
            try:
                if int(iogrp['node_count']) > 0:
                    iogrps.append(int(iogrp['id']))
            except KeyError:
                self.handle_keyerror('lsiogrp', iogrp)
            except ValueError:
                msg = (_('Expected integer for node_count, '
                         'mcsinq lsiogrp returned: %(node)s.') %
                       {'node': iogrp['node_count']})
                raise exception.VolumeBackendAPIException(data=msg)
        return iogrps

    def get_vdisk_count_by_io_group(self):
        res = {}
        resp = self.executor.lsiogrp()
        for iogrp in resp:
            try:
                if int(iogrp['node_count']) > 0:
                    res[int(iogrp['id'])] = int(iogrp['vdisk_count'])
            except KeyError:
                self.handle_keyerror('lsiogrp', iogrp)
            except ValueError:
                msg = (_('Expected integer for node_count, '
                         'mcsinq lsiogrp returned: %(node)s') %
                       {'node': iogrp['node_count']})
                raise exception.VolumeBackendAPIException(data=msg)
        return res

    def select_io_group(self, state, opts):
        selected_iog = 0
        iog_list = InStorageAssistant.get_valid_requested_io_groups(
            state, opts)
        if len(iog_list) == 0:
            raise exception.InvalidInput(
                reason=_('Given I/O group(s) %(iogrp)s not valid; available '
                         'I/O groups are %(avail)s.')
                % {'iogrp': opts['iogrp'],
                   'avail': state['available_iogrps']})
        iog_vdc = self.get_vdisk_count_by_io_group()
        LOG.debug("IO group current balance %s", iog_vdc)
        min_vdisk_count = iog_vdc[iog_list[0]]
        selected_iog = iog_list[0]
        for iog in iog_list:
            if iog_vdc[iog] < min_vdisk_count:
                min_vdisk_count = iog_vdc[iog]
                selected_iog = iog
        LOG.debug("Selected io_group is %d", selected_iog)
        return selected_iog

    def get_volume_io_group(self, vol_name):
        vdisk = self.executor.lsvdisk(vol_name)
        if vdisk:
            resp = self.executor.lsiogrp()
            for iogrp in resp:
                if iogrp['name'] == vdisk['IO_group_name']:
                    return int(iogrp['id'])
        return None

    def add_ip_addrs(self, storage_nodes):
        """Add IP addresses to system node information."""
        resp = self.executor.lsportip()
        # if NAS engine started. Storage will set up some ips for NAS cluster.
        # And this ips can not be used for iscsi or rdma. We will filter it
        filter_ips_v4 = ['172.16.2.33','172.16.2.43','172.16.2.53',
                         '172.16.2.32','172.16.2.42','172.16.2.52']
        for ip_data in resp:
            try:
                state = ip_data['state']
                if ip_data['node_id'] in storage_nodes and (
                        state == 'configured' or state == 'online'):
                    node = storage_nodes[ip_data['node_id']]
                    if len(ip_data['IP_address']) and ip_data['IP_address'] not in filter_ips_v4:
                        node['ipv4'].append(ip_data['IP_address'])
                    if len(ip_data['IP_address_6']):
                        node['ipv6'].append(ip_data['IP_address_6'])
            except KeyError:
                self.handle_keyerror('lsportip', ip_data)

    def add_fc_wwpns(self, storage_nodes):
        """Add FC WWPNs to system node information."""
        for key in storage_nodes:
            node = storage_nodes[key]
            wwpns = set(node['WWPN'])
            resp = self.executor.lsportfc(node_id=node['id'])
            for port_info in resp:
                if (port_info['type'] == 'fc' and
                        port_info['status'] == 'active'):
                    wwpns.add(port_info['WWPN'])
            node['WWPN'] = list(wwpns)
            LOG.info(_('WWPN on node %(node)s: %(wwpn)s.'),
                     {'node': node['id'], 'wwpn': node['WWPN']})

    def get_conn_fc_wwpns(self, host):
        wwpns = set()
        resp = self.executor.lsfabric(host=host)
        for wwpn in resp.select('local_wwpn'):
            if wwpn is not None:
                wwpns.add(wwpn)
        return list(wwpns)

    def add_chap_secret_to_host(self, host_name):
        """Generate and store a randomly-generated CHAP secret for the host."""
        chap_secret = volume_utils.generate_password()
        self.executor.add_chap_secret(chap_secret, host_name)
        return chap_secret

    def get_chap_secret_for_host(self, host_name):
        """Generate and store a randomly-generated CHAP secret for the host."""
        resp = self.executor.lsiscsiauth()
        host_found = False
        for host_data in resp:
            try:
                if host_data['name'] == host_name:
                    host_found = True
                    if host_data['iscsi_auth_method'] == 'chap':
                        return host_data['iscsi_chap_secret']
            except KeyError:
                self.handle_keyerror('lsiscsiauth', host_data)
        if not host_found:
            msg = _('Failed to find host %s.') % host_name
            raise exception.VolumeBackendAPIException(data=msg)
        return None

    def get_host_from_connector(self, connector, volume_name=None):
        """Return the InStorage host described by the connector."""
        LOG.debug('Enter: get_host_from_connector: %s.', connector)

        # If we have FC information, we have a faster lookup option
        host_name = None
        if 'wwpns' in connector:
            for wwpn in connector['wwpns']:
                resp = self.executor.lsfabric(wwpn=wwpn)
                for wwpn_info in resp:
                    try:
                        if (wwpn_info['remote_wwpn'] and
                                wwpn_info['name'] and
                                wwpn_info['remote_wwpn'].lower() ==
                                wwpn.lower()):
                            host_name = wwpn_info['name']
                            break
                    except KeyError:
                        self.handle_keyerror('lsfabric', wwpn_info)
                if host_name:
                    break
        if host_name:
            LOG.debug('Leave: get_host_from_connector: host %s.', host_name)
            return host_name

        def update_host_list(host, host_list):
            idx = host_list.index(host)
            del host_list[idx]
            host_list.insert(0, host)

        # That didn't work, so try exhaustive search
        hosts_info = self.executor.lshost()
        host_list = list(hosts_info.select('name'))
        # If we have a "real" connector, we might be able to find the
        # host entry with fewer queries if we move the host entries
        # that contain the connector's host property value to the front
        # of the list
        if 'host' in connector:
            # order host_list such that the host entries that
            # contain the connector's host name are at the
            # beginning of the list
            for host in host_list:
                if re.search(connector['host'], host):
                    update_host_list(host, host_list)
        # If we have a volume name we have a potential fast path
        # for finding the matching host for that volume.
        # Add the host_names that have mappings for our volume to the
        # head of the list of host names to search them first
        if volume_name:
            hosts_map_info = self.executor.lsvdiskhostmap(volume_name)
            hosts_map_info_list = list(hosts_map_info.select('host_name'))
            # remove the fast path host names from the end of the list
            # and move to the front so they are only searched for once.
            for host in hosts_map_info_list:
                update_host_list(host, host_list)
        found = False
        for name in host_list:
            try:
                resp = self.executor.lshost(host=name)
            except exception.VolumeBackendAPIException as ex:
                LOG.debug("Exception message: %s" % ex.msg)
                if 'CMMVC5754E' in ex.msg:
                    LOG.debug("CMMVC5754E found in CLI exception.")
                    # CMMVC5754E: The specified object does not exist
                    # The host has been deleted while walking the list.
                    # This is a result of a host change on the MCS that
                    # is out of band to this request.
                    continue
                # unexpected error so reraise it
                with excutils.save_and_reraise_exception():
                    pass
            if 'initiator' in connector:
                for iscsi in resp.select('iscsi_name'):
                    if iscsi == connector['initiator']:
                        host_name = name
                        found = True
                        break
            elif 'wwpns' in connector and len(connector['wwpns']):
                connector_wwpns = [str(x).lower() for x in connector['wwpns']]
                for wwpn in resp.select('WWPN'):
                    if wwpn and wwpn.lower() in connector_wwpns:
                        host_name = name
                        found = True
                        break
            elif 'nqn' in connector:
                for nqn in resp.select('nqn'):
                    if nqn == connector['nqn']:
                        host_name = name
                        found = True
                        break
            if found:
                break

        LOG.debug('Leave: get_host_from_connector: host %s.', host_name)
        return host_name

    def create_host(self, connector, site_name=''):
        """Create a new host on the storage system.

        We create a host name and associate it with the given connection
        information.  The host name will be a cleaned up version of the given
        host name (at most 55 characters), plus a random 8-character suffix to
        avoid collisions. The total length should be at most 63 characters.
        """
        LOG.debug('Enter: create_host: host %s.', connector['host'])

        # Before we start, make sure host name is a string and that we have
        # one port at least .
        host_name = connector['host']
        if not isinstance(host_name, six.string_types):
            msg = _('create_host: Host name is not unicode or string.')
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        ports = []
        if 'initiator' in connector:
            ports.append(['initiator', '%s' % connector['initiator']])
            protocol = 'scsi'
        if 'wwpns' in connector:
            for wwpn in connector['wwpns']:
                ports.append(['wwpn', '%s' % wwpn])
            protocol = 'scsi'
        if 'nqn' in connector:
            ports.append(['nqn', '%s' % connector['nqn']])
            protocol = 'nvme'
        if not ports:
            msg = _('create_host: No initiators or wwpns supplied.')
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        # Build a host name for the InStorage host - first clean up the name
        if isinstance(host_name, six.text_type):
            host_name = unicodedata.normalize('NFKD', host_name).encode(
                'ascii', 'replace').decode('ascii')

        for num in range(0, 128):
            char = str(chr(num))
            if not char.isalnum() and char not in [' ', '.', '-', '_']:
                host_name = host_name.replace(char, '-')

        # InStorage doesn't expect hostname that doesn't starts with letter or
        # _.
        if not re.match('^[A-Za-z]', host_name):
            host_name = '_' + host_name

        # Add a random 8-character suffix to avoid collisions
        rand_id = str(random.randint(0, 99999999)).zfill(8)
        host_name = '%s-%s' % (host_name[:55], rand_id)

        # Create a host with one port
        port = ports.pop(0)
        self.executor.mkhost(host_name, port[0], port[1], site_name, protocol)

        # Add any additional ports to the host
        for port in ports:
            self.executor.addhostport(host_name, port[0], port[1])

        LOG.debug('Leave: create_host: host %(host)s - %(host_name)s.',
                  {'host': connector['host'], 'host_name': host_name})
        return host_name

    def delete_host(self, host_name):
        self.executor.rmhost(host_name)

    def check_host_mapped_vols(self, host_name):
        return self.executor.lshostvdiskmap(host_name)

    def map_vol_to_host(self, volume_name, host_name, multihostmap, protocol='scsi'):
        """Create a mapping between a volume to a host."""

        LOG.debug('Enter: map_vol_to_host: volume %(volume_name)s to '
                  'host %(host_name)s.protocol: %(protocol)s',
                  {'volume_name': volume_name,
                   'host_name': host_name,
                   'protocol' : protocol}
                 )
        if protocol == 'nvme':
             # Check if this volume is already mapped to this host
            is_mapped = self.executor.check_volume_mapped(volume_name, host_name)
            if not is_mapped:
                self.executor.mkvdiskhostmap(host_name, volume_name, None,
                                                    multihostmap, protocol)
            LOG.debug('Leave: map_vol_to_host: volume '
                    '%(volume_name)s, host %(host_name)s, protocol: %(protocol)s.',
                    {'volume_name': volume_name,
                    'host_name': host_name,
                    'protocol': protocol})
            return True
        else:
            # Check if this volume is already mapped to this host
            iogrp_lun_id_map = self.executor.get_vdiskhostmapid(
                volume_name, host_name)
            if len(iogrp_lun_id_map) == 0:
                iogrp_lun_id_map = self.executor.mkvdiskhostmap(
                    host_name, volume_name, None, multihostmap)

            LOG.debug('Leave: map_vol_to_host: iogrp_lun_id_map %(iogrp_lun_id_map)s, volume '
                    '%(volume_name)s, host %(host_name)s, protocol: %(protocol)s',
                    {'iogrp_lun_id_map': iogrp_lun_id_map,
                    'volume_name': volume_name,
                    'host_name': host_name,
                    'protocol': protocol})
            return iogrp_lun_id_map

    def unmap_vol_from_host(self, volume_name, host_name):
        """Unmap the volume and delete the host if it has no more mappings."""

        LOG.debug('Enter: unmap_vol_from_host: volume %(volume_name)s from '
                  'host %(host_name)s.',
                  {'volume_name': volume_name, 'host_name': host_name})

        # Check if the mapping exists
        resp = self.executor.lsvdiskhostmap(volume_name)
        if not resp:
            LOG.warning(_('unmap_vol_from_host: No mapping of volume '
                          '%(vol_name)s to any host found.'),
                        {'vol_name': volume_name})
            return host_name
        if host_name is None:
            if len(resp) > 1:
                LOG.warning(_('unmap_vol_from_host: Multiple mappings of '
                              'volume %(vol_name)s found, no host '
                              'specified.'), {'vol_name': volume_name})
                return
            else:
                host_name = resp[0]['host_name']
        else:
            found = False
            for item in resp.select('host_name'):
                if item == host_name:
                    found = True
            if not found:
                LOG.warning(_('unmap_vol_from_host: No mapping of volume '
                              '%(vol_name)s to host %(host)s found.'),
                            {'vol_name': volume_name, 'host': host_name})
                return host_name
        # We now know that the mapping exists
        self.executor.rmvdiskhostmap(host_name, volume_name)

        LOG.debug('Leave: unmap_vol_from_host: volume %(volume_name)s from '
                  'host %(host_name)s.',
                  {'volume_name': volume_name, 'host_name': host_name})
        return host_name

    @staticmethod
    def build_default_opts(config):
        # Ignore capitalization
        opt = {
            'rsize': config.instorage_mcs_vol_rsize,
            'warning': config.instorage_mcs_vol_warning,
            'autoexpand': config.instorage_mcs_vol_autoexpand,
            'grainsize': config.instorage_mcs_vol_grainsize,
            'compressed': config.instorage_mcs_vol_compression,
            'deduped': config.instorage_mcs_vol_deduplicate,
            'intier': config.instorage_mcs_vol_intier,
            'iogrp': config.instorage_mcs_vol_iogrp,
            'access_iogrp':config.instorage_mcs_vol_access_iogrp,
            'syncrate': config.instorage_mcs_syncrate,
            'localcopyrate': config.instorage_mcs_localcopy_rate,
            'cleanrate':config.instorage_mcs_localcopy_clean_rate,
            'aa': config.instorage_mcs_enable_aa,
            'qos': None,
            'replication': False,
            'node': None
        }
        return opt

    @staticmethod
    def check_vdisk_opts(state, opts):
        # Check that grainsize is 32/64/128/256
        if opts['grainsize'] not in [32, 64, 128, 256]:
            raise exception.InvalidInput(
                reason=_('Illegal value specified for '
                         'instorage_mcs_vol_grainsize: set to either '
                         '32, 64, 128, or 256.'))

        # Check that compression is supported
        if opts['compressed'] and not state['compression_enabled']:
            raise exception.InvalidInput(
                reason=_('System does not support compression.'))

        # Check that deduped is supported
        if opts['deduped'] and not state['deduped_enabled']:
            raise exception.InvalidInput(
                reason=_('System does not support deduped.'))

        if opts['compressed'] and (opts['rsize'] == -1 or opts['rsize'] == 100):
            opts['rsize'] = 2

        if opts['deduped'] and (opts['rsize'] == -1 or opts['rsize'] == 100):
            opts['rsize'] = 2

        iogs = InStorageAssistant.get_valid_requested_io_groups(state, opts)

        if len(iogs) == 0:
            raise exception.InvalidInput(
                reason=_('Given I/O group(s) %(iogrp)s not valid; available '
                         'I/O groups are %(avail)s.')
                % {'iogrp': opts['iogrp'],
                   'avail': state['available_iogrps']})

    @staticmethod
    def get_valid_requested_io_groups(state, opts):
        given_iogs = str(opts['iogrp'])
        iog_list = given_iogs.split(',')
        # convert to int
        iog_list = list(map(int, iog_list))
        LOG.debug("Requested iogroups %s", iog_list)
        LOG.debug("Available iogroups %s", state['available_iogrps'])
        filtiog = set(iog_list).intersection(state['available_iogrps'])
        iog_list = list(filtiog)
        LOG.debug("Filtered (valid) requested iogroups %s", iog_list)
        return iog_list

    def _get_opts_from_specs(self, opts, specs):
        qos = {}
        for k, value in specs.items():
            # Get the scope, if using scope format
            key_split = k.split(':')
            if len(key_split) == 1:
                scope = None
                key = key_split[0]
            else:
                scope = key_split[0]
                key = key_split[1]

            # We generally do not look at capabilities in the driver, but
            # replication is a special case where the user asks for
            # a volume to be replicated, and we want both the scheduler and
            # the driver to act on the value.
            if not scope or scope == 'capabilities':
                if  key == 'replication' or key == 'compressed' or key == 'deduped':
                    # scope = None
                    # key = 'replication'
                    words = value.split()
                    if not (words and len(words) == 2 and words[0] == '<is>'):
                        LOG.error(_('%(key)s must be specified as '
                                    '\'<is> True\' or \'<is> False\'.') %
                                     {'key': key})
                    del words[0]
                    value = words[0]

            # Add the QoS.
            if scope and scope == 'qos':
                if key in self.mcs_qos_keys:
                    try:
                        type_fn = self.mcs_qos_keys[key]['type']
                        value = type_fn(value)
                        qos[key] = value
                    except ValueError:
                        continue

            # Any keys that the driver should look at should have the
            # 'drivers' scope.
            if key in opts:
                this_type = type(opts[key]).__name__
                if this_type == 'int':
                    value = int(value)
                elif this_type == 'bool':
                    value = strutils.bool_from_string(value)
                opts[key] = value
        if len(qos) != 0:
            opts['qos'] = qos
        return opts

    def _get_qos_from_volume_metadata(self, volume_metadata):
        """Return the QoS information from the volume metadata."""
        qos = {}
        for i in volume_metadata:
            k = i.get('key', None)
            value = i.get('value', None)
            key_split = k.split(':')
            if len(key_split) == 1:
                scope = None
                key = key_split[0]
            else:
                scope = key_split[0]
                key = key_split[1]
            # Add the QoS.
            if scope and scope == 'qos':
                if key in self.mcs_qos_keys:
                    try:
                        type_fn = self.mcs_qos_keys[key]['type']
                        value = type_fn(value)
                        qos[key] = value
                    except ValueError:
                        continue
        return qos

    def wait_for_a_condition(self, testmethod, timeout=None,
                              interval=INTERVAL_1_SEC,
                              raise_exception=False):
        start_time = time.time()
        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        def _inner():
            try:
                test_value = testmethod()
            except Exception as ex:
                if raise_exception:
                    LOG.exception(_("wait_for_a_condition: %s"
                                    " execution failed."),
                                  testmethod.__name__)
                    raise exception.VolumeBackendAPIException(data=ex)
                else:
                    test_value = False
                    LOG.debug('Assistant.'
                              '_wait_for_condition: %(method_name)s '
                              'execution failed for %(exception)s.',
                              {'method_name': testmethod.__name__,
                               'exception': ex.message})
            if test_value:
                raise loopingcall.LoopingCallDone()

            if int(time.time()) - start_time > timeout:
                msg = (
                    _('CommandLineAssistant._wait_for_condition: '
                      '%s timeout.') % testmethod.__name__)
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        timer = loopingcall.FixedIntervalLoopingCall(_inner)
        timer.start(interval=interval).wait()

    def get_vdisk_params(self, config, state, type_id,
                         volume_type=None, volume_metadata=None):
        """Return the parameters for creating the vdisk.

        Get volume type and defaults from config options
        and take them into account.
        """
        opts = self.build_default_opts(config)
        ctxt = context.get_admin_context()
        if volume_type is None and type_id is not None:
            volume_type = volume_types.get_volume_type(ctxt, type_id)
        if volume_type:
            qos_specs_id = volume_type.get('qos_specs_id')
            specs = dict(volume_type).get('extra_specs')

            # NOTE: We prefer the qos_specs association
            # and over-ride any existing
            # extra-specs settings if present
            if qos_specs_id is not None:
                kvs = qos_specs.get_qos_specs(ctxt, qos_specs_id)['specs']
                # Merge the qos_specs into extra_specs and qos_specs has higher
                # priority than extra_specs if they have different values for
                # the same key.
                specs.update(kvs)
            opts = self._get_opts_from_specs(opts, specs)
        if (opts['qos'] is None and config.instorage_mcs_allow_tenant_qos and
                volume_metadata):
            qos = self._get_qos_from_volume_metadata(volume_metadata)
            if len(qos) != 0:
                opts['qos'] = qos

        self.check_vdisk_opts(state, opts)
        return opts

    @staticmethod
    def _get_vdisk_create_params(opts, is_ssa_pool=False):
        params = []
        params.extend(['-syncrate', str(opts['syncrate'])])
        if is_ssa_pool or (opts['rsize'] != -1 and opts['rsize'] != 100):
            if opts['rsize'] == -1 or opts['rsize'] == 100:
                opts['rsize'] = 2
            params.extend(['-rsize', '%s%%' % str(opts['rsize'])])
            if opts['autoexpand']:
                params.append('-autoexpand')
            params.extend(['-warning', '%s%%' % str(opts['warning'])])

        if is_ssa_pool:
            if opts['deduped']:
                params.append('-deduped')
            if opts['compressed']:
                params.append('-compressed')
        elif opts['rsize'] != -1 and opts['rsize'] != 100:
            if opts['compressed']:
                params.append('-compressed')
            else:
                params.extend(['-grainsize', str(opts['grainsize'])])

        # intier option is ignore when pool is ssa
        if not is_ssa_pool:
            intier = 'on' if opts['intier'] else 'off'
            params.extend(['-intier', intier])

        return params

    @staticmethod
    def _get_volume_create_params(opts, is_ssa_pool=False):
        params = []
        if is_ssa_pool or (opts['rsize'] != -1 and opts['rsize'] != 100):
            if opts['rsize'] in [-1, 100]:
                #  if the configured pool is ssa and rsize is not configured
                #  we will set the rsize to 2
                opts['rsize'] = 2
            params.extend(['-buffersize', '%s%%' % str(opts['rsize']),
                           '-warning', '%s%%' % str(opts['warning'])])
            if not opts['autoexpand']:
                params.append('-noautoexpand')

        if is_ssa_pool:
            params.append('-thin')
            if opts['deduped']:
                params.append('-deduped')
            if opts['compressed']:
                params.append('-compressed')
        elif opts['rsize'] != -1 and opts['rsize'] != 100:
            if opts['compressed']:
                params.append('-compressed')
            else:
                params.append('-thin')
                params.extend(['-grainsize', str(opts['grainsize'])])

        # thick volume has no other params
        return params

    def check_ssa_pool_params(self, opts):
        """Check the configured parameters if vol in ssa pool."""
        if opts['rsize'] == -1 or opts['rsize'] == 100:
            opts['rsize'] = 2

    def check_normal_pool_params(self,opts):
        """Check the configured parameters if vol in normal pool."""
        if opts['deduped']:
            msg = (_('You cannot create deduped volume in normal pool'))
            raise exception.VolumeDriverException(message=msg)
        if opts['compressed'] and (opts['rsize'] == -1 or opts['rsize'] == 100):
            opts['rsize'] = 2

    def create_vdisk(self, name, size, units, pool, opts, nofmtdisk=False):
        LOG.debug('we will create a general volume %s' % name)

        name = '"%s"' % name

        is_ssa_pool = self.is_ssa_pool(pool)
        if is_ssa_pool:
            self.check_ssa_pool_params(opts)
        else:
            self.check_normal_pool_params(opts)
        params = self._get_vdisk_create_params(opts, is_ssa_pool)

        self.executor.mkvdisk(name, size, units, pool, opts, params, nofmtdisk)

    def create_volume(self, name, size, units, pool, opts):
        LOG.debug('we will create an active-active volume %s' % name)

        name = '"%s"' % name
        is_ssa_pool = self.is_ssa_pool(pool)
        if is_ssa_pool:
            self.check_ssa_pool_params(opts)
        else:
            self.check_normal_pool_params(opts)
        params = self._get_volume_create_params(opts, is_ssa_pool)

        poollist = '%s:%s' % (pool, self.pool_map[pool])
        iogrp = '%s' % opts['iogrp']
        iogrplist = '%s:%s' % (iogrp, self.iogrp_map[iogrp])
        self.executor.mkvolume(name, size, units, poollist, iogrplist, params)

    def transfer_vdisk_to_volume(self, name, pool, opts):
        LOG.debug('we will transfter vdisk %s to active-active volume.' % name)

        name = '"%s"' % name

        params = self._get_volume_create_params(opts, self.is_ssa_pool(pool))
        dst_pool = self.pool_map[pool]
        dst_iogrp = self.iogrp_map[opts['iogrp']]
        self.executor.addvolumecopy(name, dst_pool, dst_iogrp, params)

    def transfer_volume_to_vdisk(self, name, pool):
        LOG.debug('we will transfer volume %s to general vdisk by delete a copy.' % name)

        name = '"%s"' % name
        self.executor.rmvolumecopy(name, self.pool_map[pool])

    # maybe change a name is better, in this function itself will know
    # whether it is a aa volume or a general vdisk
    def delete_vdisk(self, vdisk, force):
        """Ensures that vdisk is not part of FC mapping and deletes it."""
        LOG.debug('Enter: delete_vdisk: vdisk %s.', vdisk)
        if not self.is_vdisk_defined(vdisk):
            LOG.info(_('Tried to delete non-existent vdisk %s.'), vdisk)
            return

        if self.is_aa_vdisk(vdisk):
            LOG.debug('Remove active-active volume')
            self.executor.rmvolume(vdisk, force=force)
        else:
            LOG.debug('Remove general vdisk')
            # however we always need to promise it has no local copy
            self.ensure_vdisk_no_lc_mappings(vdisk, allow_snaps=True, allow_lctgt=True)

            self.executor.rmvdisk(vdisk, force=force)
        LOG.debug('Leave: delete_vdisk: vdisk %s.', vdisk)

    def is_aa_vdisk(self, vdisk_name):
        attrs = self.executor.lsvdisks_from_filter('volume_name', vdisk_name)
        return True if len(attrs) == 4 else False

    def is_vdisk_defined(self, vdisk_name):
        """Check if vdisk is defined."""
        attrs = self.get_vdisk_attributes(vdisk_name)
        return attrs is not None

    def get_vdisk_attributes(self, vdisk):
        attrs = self.executor.lsvdisk(vdisk)
        return attrs

    def get_volumes(self, pool_name):
        """get all volumes in a specify pool"""
        return self.executor.lsvdisks_from_filter('mdisk_grp_name', pool_name)

    def get_volume_pool_iogrp_map(self, volume_name):
        attrs = self.executor.lsvdisks_from_filter('volume_name', volume_name)
        master = None
        auxiliary = None
        for attr in attrs:
            subattr = {
                'mdisk_grp_name': attr['mdisk_grp_name'],
                'IO_group_id': attr['IO_group_id']
            }
            if attr['function'] == 'master':
                master = subattr
            elif attr['function'] == 'aux':
                auxiliary = subattr

        return master, auxiliary

    def get_volume_iogrps(self, volume_name):
        attrs = self.executor.lsvdisks_from_filter('volume_name', volume_name)
        iogrps = set()
        for attr in attrs:
            iogrps.add(attr['IO_group_id'])
        return iogrps

    def get_volume_prefer_site_name(self, volume_name):
        site_name = ''
        prefer_iogrp_name = None

        attrs = self.executor.lsvdisks_from_filter('volume_name', volume_name)
        topology = self.get_system_info()['topology']
        for attr in attrs:
            if attr['function'] == 'master':
                prefer_iogrp_name = attr['IO_group_name']
                break
            if topology == 'inmetro':
                prefer_iogrp_name = attr['IO_group_name']
                break

        if prefer_iogrp_name is None:
            return site_name

        iogrps = self.executor.lsiogrp()
        for iogrp in iogrps:
            if iogrp['name'] == prefer_iogrp_name:
                site_name = iogrp['site_name']
                break
        return site_name

    def get_volumes_used_size(self, volume_name=None, pool=None):
        result = {}
        if volume_name:
            try:
                resp = self.executor.lsvdiskcopy(volume_name, None, None)[0]
                volume_id = volume_name[7:]
                result.update({volume_id: int(resp['used_capacity'])})
            except Exception as ex:
                if 'CMMVC5804E' in ex.msg:
                    LOG.warning('could not find the  %(vdisk)s.',
                            {'vdisk': volume_name})
                    return None
        elif pool:
            try:
                resp = self.executor.lsvdiskcopy(None, None, pool)
                for vdisk_name, used_capacity in resp.select(
                        'vdisk_name', 'used_capacity'):
                    if not vdisk_name.startswith('volume-'):
                        continue
                    volume_id = vdisk_name[7:]
                    result.update({volume_id: int(used_capacity)})
            except Exception:
                return None
        return result


    def find_vdisk_copy_id(self, vdisk, pool):
        resp = self.executor.lsvdiskcopy(vdisk)
        for copy_id, mdisk_grp in resp.select('copy_id', 'mdisk_grp_name'):
            if mdisk_grp == pool:
                return copy_id
        msg = _('Failed to find a vdisk copy in the expected pool.')
        LOG.error(msg)
        raise exception.VolumeDriverException(message=msg)

    def get_vdisk_copy_attrs(self, vdisk, copy_id):
        try:
            return self.executor.lsvdiskcopy(vdisk, copy_id=copy_id)[0]
        except Exception as ex:
            if 'CMMVC5804E' in ex.msg:
                LOG.warning('could not find the copy%(copyid)s of %(vdisk)s.',
                         {'copyid': copy_id, 'vdisk': vdisk})
                return None

    def get_vdisk_copies(self, vdisk):
        copies = {'primary': None,
                  'secondary': None}

        resp = self.executor.lsvdiskcopy(vdisk)
        for copy_id, status, sync, primary, mdisk_grp in (
            resp.select('copy_id', 'status', 'sync',
                        'primary', 'mdisk_grp_name')):
            copy = {'copy_id': copy_id,
                    'status': status,
                    'sync': sync,
                    'primary': primary,
                    'mdisk_grp_name': mdisk_grp,
                    'sync_progress': None}
            if copy['sync'] != 'yes':
                try:
                    progress_info = self.executor.lsvdisksyncprogress(vdisk, copy_id)
                    copy['sync_progress'] = progress_info['progress']
                except Exception as ex:
                    if 'CMMVC5804E' in ex.msg:
                        copy['sync_progress'] = 100
            else:
                copy['sync_progress'] = 100
            if copy['primary'] == 'yes':
                copies['primary'] = copy
            else:
                copies['secondary'] = copy
        return copies

    def create_copy(self, src, tgt, src_id, config, opts,
                    full_copy, pool=None, nofmtdisk=False):
        """Create a new snapshot using LocalCopy."""
        LOG.debug('Enter: create_copy: snapshot %(src)s to %(tgt)s.',
                  {'tgt': tgt, 'src': src})

        src_attrs = self.get_vdisk_attributes(src)
        if src_attrs is None:
            msg = (_('create_copy: Source vdisk %(src)s (%(src_id)s) '
                     'does not exist.') % {'src': src, 'src_id': src_id})
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        src_size = src_attrs['capacity']
        src_node = src_attrs['preferred_node_id']
        if src_node and int(src_node) != 0:
            opts['node'] = src_node
        pool = src_attrs['mdisk_grp_name']
        opts['iogrp'] = src_attrs['IO_group_id']
        self.create_vdisk(tgt, src_size, 'b', pool, opts, nofmtdisk)
        timeout = config.instorage_mcs_localcopy_timeout
        try:
            self.run_localcopy(src, tgt, timeout,
                               opts['localcopyrate'],
                               opts['cleanrate'],
                               full_copy=full_copy)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.delete_vdisk(tgt, True)

        LOG.debug('Leave: _create_copy: snapshot %(tgt)s from '
                  'vdisk %(src)s.',
                  {'tgt': tgt, 'src': src})

    def extend_vdisk(self, vdisk, amount):
        self.executor.expandvdisksize(vdisk, amount)

    def add_vdisk_copy(self, vdisk, dest_pool, volume_type, state, config):
        """Add a vdisk copy in the given pool."""
        resp = self.executor.lsvdiskcopy(vdisk)
        if len(resp) > 1:
            msg = (_('add_vdisk_copy failed: A copy of volume %s exists. '
                     'Adding another copy would exceed the limit of '
                     '2 copies.') % vdisk)
            raise exception.VolumeDriverException(message=msg)
        orig_copy_id = resp[0].get("copy_id", None)

        if orig_copy_id is None:
            msg = (_('add_vdisk_copy started without a vdisk copy in the '
                     'expected pool.'))
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        if volume_type is None:
            opts = self.get_vdisk_params(config, state, None)
        else:
            opts = self.get_vdisk_params(config, state, volume_type['id'],
                                         volume_type=volume_type)
        params = self._get_vdisk_create_params(opts, self.is_ssa_pool(dest_pool))
        new_copy_id = self.executor.addvdiskcopy(vdisk, dest_pool, params)
        return (orig_copy_id, new_copy_id)

    def check_vdisk_copy_synced(self, vdisk, copy_id):
        sync = self.executor.lsvdiskcopy(vdisk, copy_id=copy_id)[0]['sync']
        if sync == 'yes':
            return True
        return False

    def rm_vdisk_copy(self, vdisk, copy_id):
        self.executor.rmvdiskcopy(vdisk, copy_id)

    def _prepare_lc_map(self, lc_map_id, timeout, restore=False):
        self.executor.prestartlcmap(lc_map_id, restore)
        mapping_ready = False
        max_retries = (timeout // self.WAIT_TIME) + 1
        for _ in range(1, max_retries):
            mapping_attrs = self.get_localcopy_mapping_attributes(lc_map_id)
            if (mapping_attrs is None or
                    'status' not in mapping_attrs):
                break
            if mapping_attrs['status'] == 'prepared':
                mapping_ready = True
                break
            elif mapping_attrs['status'] == 'stopped':
                self.executor.prestartlcmap(lc_map_id, restore)
            elif mapping_attrs['status'] != 'preparing':
                msg = (_('Unexecpted mapping status %(status)s for mapping '
                         '%(id)s. Attributes: %(attr)s.')
                       % {'status': mapping_attrs['status'],
                          'id': lc_map_id,
                          'attr': mapping_attrs})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            greenthread.sleep(self.WAIT_TIME)

        if not mapping_ready:
            msg = (_('Mapping %(id)s prepare failed to complete within the'
                     'allotted %(to)d seconds timeout. Terminating.')
                   % {'id': lc_map_id,
                      'to': timeout})
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

    def stop_lcmap(self, lc_map_id, split=False):
        self.executor.stoplcmap(lc_map_id, split)

    def rm_lcmap(self, lc_map_id):
        self.executor.rmlcmap(lc_map_id)

    # Consistency Group
    def start_lc_consistgrp(self, lc_consistgrp):
        self.executor.startlcconsistgrp(lc_consistgrp)

    def create_lc_consistgrp(self, lc_consistgrp, config):
        if not config.instorage_mcs_fastconsistgrp_support:
            self.executor.mklcconsistgrp(lc_consistgrp)
        else:
            self.executor.mklcconsistgrp(lc_consistgrp, True)

    def delete_lc_consistgrp(self, lc_consistgrp):
        self.executor.rmlcconsistgrp(lc_consistgrp)

    def stop_lc_consistgrp(self, lc_consistgrp):
        self.executor.stoplcconsistgrp(lc_consistgrp)

    def run_group_snapshots(self, lc_group, snapshots, state,
                            config, timeout, snap_name_generator, vol_name_generator):
        model_update = {'status': 'available'}
        snapshots_model_update = []
        try:
            for snapshot in snapshots:
                opts = self.get_vdisk_params(config, state,
                                             snapshot['volume_type_id'])

                snap_name = snap_name_generator(snapshot)
                snap_vol_name = vol_name_generator(snapshot['volume'])
                opts['rsize'] = 0
                self.create_localcopy_to_consistgrp(snap_vol_name,
                                                    snap_name,
                                                    lc_group,
                                                    opts)

            self.prepare_lc_consistgrp(lc_group, timeout)
            self.start_lc_consistgrp(lc_group)
            # There is CG limitation that could not create more than 128 CGs.
            # After start CG, we delete CG to avoid CG limitation.
            # Cinder general will maintain the group and snapshots
            # relationship.
            self.delete_lc_consistgrp(lc_group)
        except exception.VolumeBackendAPIException as err:
            model_update['status'] = 'error'
            # Release cg
            self.delete_lc_consistgrp(lc_group)
            LOG.error(_("Failed to create Group_Snapshot. "
                        "Exception: %s."), err)

        for snapshot in snapshots:
            snapshots_model_update.append(
                {'id': snapshot['id'],
                 'status': model_update['status'],
                 'replication_status': 'disabled'})

        return model_update, snapshots_model_update

    def delete_group_snapshots(self, lc_group, snapshots, snap_name_generator):
        """Delete localcopy maps and group."""
        model_update = {'status': 'deleted'}
        snapshots_model_update = []

        try:
            for snapshot in snapshots:
                snap_name = snap_name_generator(snapshot)
                if self.get_vdisk_attributes(snap_name) is not None:
                    self.executor.rmvdisk(snap_name, True)
        except exception.VolumeBackendAPIException as err:
            model_update['status'] = 'error_deleting'
            LOG.error(_("Failed to delete the snapshot %(snap)s of "
                        "Group_Snapshot. Exception: %(exception)s."),
                      {'snap': snapshot['id'], 'exception': err})

        for snapshot in snapshots:
            snapshots_model_update.append(
                {'id': snapshot['id'],
                 'status': model_update['status']})

        return model_update, snapshots_model_update

    def prepare_lc_consistgrp(self, lc_consistgrp, timeout):
        """Prepare LC Consistency Group."""
        self.executor.prestartlcconsistgrp(lc_consistgrp)

        def prepare_lc_consistgrp_success():
            mapping_ready = False
            mapping_attrs = self._get_localcopy_consistgrp_attr(lc_consistgrp)
            if (mapping_attrs is None or
                    'status' not in mapping_attrs):
                return mapping_ready
            if mapping_attrs['status'] == 'prepared':
                mapping_ready = True
            elif mapping_attrs['status'] == 'stopped':
                self.executor.prestartlcconsistgrp(lc_consistgrp)
            elif mapping_attrs['status'] != 'preparing':
                msg = (_('Unexpected mapping status %(status)s for mapping'
                         '%(id)s. Attributes: %(attr)s.') %
                       {'status': mapping_attrs['status'],
                        'id': lc_consistgrp,
                        'attr': mapping_attrs})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            return mapping_ready
        self.wait_for_a_condition(prepare_lc_consistgrp_success, timeout)

    def create_group_from_source(self, group, lc_group,
                                 sources, targets, state,
                                 config, timeout, src_name_generator, tgt_name_generator):
        """Create group from source"""
        LOG.debug('Enter: create_group_from_source: group %(group)s'
                  ' source %(source)s, target %(target)s',
                  {'group': lc_group, 'source': sources, 'target': targets})
        model_update = {'status': 'available'}
        ctxt = context.get_admin_context()
        try:
            for source, target in zip(sources, targets):
                opts = self.get_vdisk_params(config, state,
                                             source['volume_type_id'])
                pool = volume_utils.extract_host(target['host'], 'pool')

                source_name = src_name_generator(source)
                target_name = tgt_name_generator(target)

                self.create_localcopy_to_consistgrp(source_name,
                                                    target_name,
                                                    lc_group,
                                                    opts,
                                                    True, pool=pool)
            self.prepare_lc_consistgrp(lc_group, timeout)
            self.start_lc_consistgrp(lc_group)
            self.delete_lc_consistgrp(lc_group)
            volumes_model_update = self._get_volume_model_updates(
                ctxt, targets, group['id'], model_update['status'])
        except exception.VolumeBackendAPIException as err:
            model_update['status'] = 'error'
            volumes_model_update = self._get_volume_model_updates(
                ctxt, targets, group['id'], model_update['status'])
            with excutils.save_and_reraise_exception():
                self.delete_lc_consistgrp(lc_group)
                LOG.error(_("Failed to create group from group_snapshot. "
                            "Exception: %s"), err)
            return model_update, volumes_model_update

        LOG.debug('Leave: create_cg_from_source.')
        return model_update, volumes_model_update

    def create_rc_consistgrp(self, group_name, system_id):
        self.executor.mkrcconsistgrp(group_name, system_id)

    def delete_rc_consistgrp(self, group_name):
        self.executor.rmrcconsistgrp(group_name)

    def chrcrelationship(self, key, rcrel, group_name=None):
        resp = self.executor.lsrcrelationship(key, rcrel)
        if resp:
            rcrel_dict = resp[0]
        else:
            with excutils.save_and_reraise_exception():
                LOG.error(_("The rcrelationship %s is not exist ,"
                            "chrcrelationship failed. "), rcrel)
        if rcrel_dict and rcrel_dict['consistency_group_name'] == group_name:
            LOG.warning('relationship %(rel)s is aleady added to group'
                        '%(grp)s.',{'rel':rcrel_dict['name'],'grp':group_name})
            return

        if not group_name and rcrel_dict['consistency_group_name'] == '':
            LOG.warning('relationship %(rel)s is aleady removed from group'
                        '%(grp)s.',{'rel':rcrel_dict['name'],'grp':group_name})
            return

        self.executor.chrcrelationship(rcrel, group_name)

    def start_rc_consistgrp(self, rccg, primary=None):
        self.executor.startrcconsistgrp(rccg, primary)

    def stop_rc_consistgrp(self, rccg, access=False):
        self.executor.stoprcconsistgrp(rccg, access)

    def _get_volume_model_updates(self, ctxt, volumes, cg_id,
                                  status='available'):
        """Update the volume model's status and return it."""
        volume_model_updates = []
        LOG.info(_(
            "Updating status for CG: %(id)s."),
            {'id': cg_id})
        if volumes:
            for volume in volumes:
                volume_model_updates.append({'id': volume['id'],
                                             'status': status})
        else:
            LOG.info(_("No volume found for CG: %(cg)s."),
                     {'cg': cg_id})
        return volume_model_updates

    def run_localcopy(self, source, target, timeout, copy_rate, clean_rate,
                      full_copy=True, restore=False):
        """Create a LocalCopy mapping from the source to the target."""
        LOG.debug('Enter: run_localcopy: execute LocalCopy from source '
                  '%(source)s to target %(target)s.',
                  {'source': source, 'target': target})

        lc_map_id = self.executor.mklcmap(source, target, full_copy, copy_rate,
                                          clean_rate)
        #self._prepare_lc_map(lc_map_id, timeout, restore)
        self.executor.startlcmap(lc_map_id, restore)

        LOG.debug('Leave: run_localcopy: LocalCopy started from '
                  '%(source)s to %(target)s.',
                  {'source': source, 'target': target})

    def create_localcopy_to_consistgrp(self, source, target, consistgrp,
                                       opts, full_copy=False, pool=None):
        """Create a LocalCopy mapping and add to consistent group."""
        LOG.debug('Enter: create_localcopy_to_consistgrp: create LocalCopy'
                  ' from source %(source)s to target %(target)s'
                  'Then add the localcopy to %(cg)s.',
                  {'source': source, 'target': target, 'cg': consistgrp})

        src_attrs = self.get_vdisk_attributes(source)
        if src_attrs is None:
            msg = (_('create_copy: Source vdisk %(src)s '
                     'does not exist.') % {'src': source})
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        src_size = src_attrs['capacity']
        opts['iogrp'] = src_attrs['IO_group_id']
        pool = src_attrs['mdisk_grp_name']
        src_node = src_attrs['preferred_node_id']
        if src_node and int(src_node) != 0:
            opts['node'] = src_node

        self.create_vdisk(target, src_size, 'b', pool, opts)

        self.executor.mklcmap(source, target, full_copy,
                         opts['localcopyrate'],
                         opts['cleanrate'],
                         consistgrp=consistgrp)

        LOG.debug('Leave: create_localcopy_to_consistgrp: '
                  'LocalCopy started from  %(source)s to %(target)s.',
                  {'source': source, 'target': target})

    def get_vdisk_lc_mappings(self, vdisk, type='all'):
        """Return LocalCopy mappings that this vdisk is associated with."""
        src_mappings = []
        tgt_mappings = []
        lcmappings = self.executor.lsvdisklcmappings(vdisk)
        for lcmapping in lcmappings:
            attrs = self.get_localcopy_mapping_attributes(lcmapping['id'])
            if attrs is None:
                continue
            if attrs['source_vdisk_name'] == vdisk and type in ['all', 'source']:
                src_mappings.append(attrs)
            elif attrs['target_vdisk_name'] == vdisk and type in ['all', 'target']:
                tgt_mappings.append(attrs)

        return src_mappings, tgt_mappings

    def get_localcopy_mapping_attributes(self, lc_map_id):
        resp = self.executor.lslcmap(lc_map_id)
        if not resp:
            return None
        return resp[0]

    def _get_localcopy_consistgrp_attr(self, lc_consistgrp):
        resp = self.executor.lslcconsistgrp(lc_consistgrp)
        if not resp:
            return None
        return resp[0]

    def get_remotecopy_consistgrp_attr(self, rc_consistgrp):
        resp = self.executor.lsrcconsistgrp(rc_consistgrp)
        if not resp:
            return None
        return resp[0]

    def _check_vdisk_lc_mappings(self, name,
                                 allow_snaps=True, allow_lctgt=False, type='all'):
        """LocalCopy mapping check helper."""
        LOG.debug('Loopcall: _check_vdisk_lc_mappings(), vdisk %s.', name)
        wait_for_copy = False
        rmlcmap_failed_e = None
        src_mappings, tgt_mappings = self.get_vdisk_lc_mappings(name,type)
        for mappings in [src_mappings, tgt_mappings]:
            for attrs in mappings:
                map_id = attrs['id']
                source = attrs['source_vdisk_name']
                target = attrs['target_vdisk_name']
                copy_rate = attrs['copy_rate']
                status = attrs['status']
                progress = attrs['progress']

                if allow_snaps and copy_rate != '0' and source == name and progress != '100':
                    return

                if allow_lctgt and target == name and status == 'copying':
                    src_mappings, _ = self.get_vdisk_lc_mappings(target, type='source')
                    if src_mappings:
                        self.executor.stoplcmap(map_id, split=True)
                    else:
                        self.executor.stoplcmap(map_id, split=False)
                    attrs = self.get_localcopy_mapping_attributes(map_id)
                    if attrs:
                        status = attrs['status']
                    else:
                        return

                if copy_rate == '0':
                    if source == name:
                        # Vdisk with snapshots. Return False if snapshot
                        # not allowed.
                        if not allow_snaps:
                            raise loopingcall.LoopingCallDone(retvalue=False)
                        self.executor.chlcmap(map_id, copyrate='100', autodel='on')
                        wait_for_copy = True
                    else:
                        # A snapshot
                        if status in ['copying', 'prepared']:
                            src_mappings, _ = self.get_vdisk_lc_mappings(target, type='source')
                            if src_mappings:
                                self.executor.stoplcmap(map_id, split=True)
                            else:
                                self.executor.stoplcmap(map_id, split=False)
                            # Need to wait for the lcmap to change to
                            # stopped state before remove lcmap
                            wait_for_copy = True
                        elif status in ['stopping', 'preparing']:
                            wait_for_copy = True
                        else:
                            try:
                                self.executor.rmlcmap(map_id)
                            except exception.VolumeBackendAPIException as exp:
                                rmlcmap_failed_e = exp
                # Case 4: Copy in progress - wait and will autodelete
                else:
                    if status == 'prepared':
                        self.executor.stoplcmap(map_id)
                        self.executor.rmlcmap(map_id)
                    elif status in ['idle_or_copied', 'stopped']:
                        # Prepare failed or stopped
                        self.executor.rmlcmap(map_id)
                    elif progress == '100' and status == 'copying':
                        src_mappings, _ = self.get_vdisk_lc_mappings(target, type='source')
                        if src_mappings:
                            self.executor.stoplcmap(map_id, split=True)
                        else:
                            self.executor.stoplcmap(map_id, split=False)
                        wait_for_copy = True
                    else:
                        wait_for_copy = True
            if wait_for_copy:
                return

        if rmlcmap_failed_e is not None:
            raise rmlcmap_failed_e

        raise loopingcall.LoopingCallDone(retvalue=True)

    def ensure_vdisk_no_lc_mappings(self, name, allow_snaps=True,
                                    allow_lctgt=False, type='all'):
        """Ensure vdisk has no localcopy mappings."""
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_vdisk_lc_mappings, name,
            allow_snaps, allow_lctgt, type)
        # Create a timer greenthread. The default volume service heart
        # beat is every 10 seconds. The localcopy usually takes hours
        # before it finishes. Don't set the sleep interval shorter
        # than the heartbeat. Otherwise volume service heartbeat
        # will not be serviced.
        LOG.debug('Calling _ensure_vdisk_no_lc_mappings: vdisk %s.',
                  name)
        ret = timer.start(interval=self.loop_check_interval).wait()
        timer.stop()
        return ret

    def _check_vdisk_formatting(self, name):
        LOG.debug('Loopcall: _check_vdisk_formatting(), vdisk %s.', name)
        formatting_count = 0
        attrs = self.executor.lsvdisks_from_filter('volume_name', name)
        for attr in attrs:
            if attr['formatting'] == 'yes':
                formatting_count += 1

        if formatting_count == 0:
            raise loopingcall.LoopingCallDone(retvalue=True)
        else:
            LOG.warning('wait for vdisk %s formatting.', name)
            return

    def ensure_vdisk_not_formatting(self, name):
        """Ensure vdisk is not formatting."""
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_vdisk_formatting, name)
        # Create a timer greenthread. The default volume service heart
        # beat is every 10 seconds. The localcopy usually takes hours
        # before it finishes. Don't set the sleep interval shorter
        # than the heartbeat. Otherwise volume service heartbeat
        # will not be serviced.
        LOG.debug('Calling _ensure_vdisk_not_formatting: vdisk %s.', name)
        ret = timer.start(interval=self.loop_check_interval).wait()
        timer.stop()
        return ret

    def _check_rollback_snapshot_mappings(self, vol_name, snap_name):
        """Rollback snapshot mapping check helper."""
        LOG.debug('Loopcall: _check_rollback_snapshot_mappings(), vdisk %s.',
                   snap_name)
        wait_for_copy = False

        src_mappings, _ = self.get_vdisk_lc_mappings(snap_name, type='source')
        for attrs in src_mappings:
            source = attrs['source_vdisk_name']
            target = attrs['target_vdisk_name']
            copy_rate = attrs['copy_rate']
            if source == snap_name and target == vol_name and copy_rate != '0':
                wait_for_copy = True
        if wait_for_copy:
            return
        raise loopingcall.LoopingCallDone(retvalue=True)

    def ensure_rollback_snapshot_ok(self, vol_name, snap_name):
        """Ensure rollback_snapshot has finished"""
        timer = loopingcall.FixedIntervalLoopingCall(
            self._check_rollback_snapshot_mappings, vol_name, snap_name
        )
        LOG.debug('Enter ensure_rollback_snapshot_ok: sourcename: %s', snap_name)
        timer.start(interval=5).wait()
        LOG.debug('Leave ensure_rollback_snapshot_ok: sourcename: %s', snap_name)

    def start_relationship(self, volume_name, primary=None):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if vol_attrs['RC_name']:
            self.executor.startrcrelationship(vol_attrs['RC_name'], primary)

    def start_rccg(self, rccg, primary=None):
        self.executor.startrcconsistgrp(rccg, primary)

    def stop_rccg(self, rccg, access=False):
        self.executor.stoprcconsistgrp(rccg, access)

    def stop_relationship(self, volume_name, access=False):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if vol_attrs['RC_name']:
            self.executor.stoprcrelationship(vol_attrs['RC_name'], access=access)

    def create_relationship(self, master, aux, system, asynccopy, rccg_name=None):
        try:
            rc_id = self.executor.mkrcrelationship(master, aux, system,
                                              asynccopy)
        except exception.VolumeBackendAPIException as exp:
            # CMMVC5959E is the code in InStorage, meaning that
            # there is a relationship that already has this name on the
            # master cluster.
            if 'CMMVC5959E' not in six.text_type(exp):
                # If there is no relation between the primary and the
                # secondary back-end storage, the exception is raised.
                raise
        if rc_id:
            self.start_relationship(master)
        if rc_id and rccg_name:
            self.chrcrelationship('id', rc_id, rccg_name)

    def delete_relationship(self, volume_name):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if vol_attrs['RC_name']:
            self.executor.rmrcrelationship(vol_attrs['RC_name'], True)

    def get_relationship_info(self, volume_name):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if not vol_attrs or not vol_attrs['RC_name']:
            LOG.debug(_("Unable to get remote copy information for "
                       "volume %s"), volume_name)
            return None

        relationship = self.executor.lsrcrelationship('name', vol_attrs['RC_name'])
        return relationship[0] if len(relationship) > 0 else None

    def delete_rc_volume(self, volume_name, target_vol=False):
        vol_name = volume_name
        if target_vol:
            vol_name = instorage_const.REPLICA_AUX_VOL_PREFIX + volume_name

        try:
            rel_info = self.get_relationship_info(vol_name)
            if rel_info:
                self.delete_relationship(vol_name)
            self.delete_vdisk(vol_name, False)
        except Exception as exp:
            msg = (_('Unable to delete the volume for '
                     'volume %(vol)s. Exception: %(err)s.'),
                   {'vol': vol_name, 'err': six.text_type(exp)})
            LOG.exception(msg)
            raise exception.VolumeDriverException(message=msg)

    def switch_relationship(self, relationship, aux=True):
        self.executor.switchrelationship(relationship, aux)

    def get_partnership_info(self, system_name):
        partnership = self.executor.lspartnership(system_name)
        return partnership[0] if len(partnership) > 0 else None

    def get_partnershipcandidate_info(self, system_name):
        candidates = self.executor.lspartnershipcandidate()
        for candidate in candidates:
            if system_name == candidate['name']:
                return candidate
        return None

    def mkippartnership(self, ip_v4, bandwith=1000, copyrate=50):
        self.executor.mkippartnership(ip_v4, bandwith, copyrate)

    def mkfcpartnership(self, system_name, bandwith=1000, copyrate=50):
        self.executor.mkfcpartnership(system_name, bandwith, copyrate)

    def chpartnership(self, partnership_id):
        self.executor.chpartnership(partnership_id)

    @staticmethod
    def can_migrate_to_host(host, state):
        if 'location_info' not in host['capabilities']:
            return None
        info = host['capabilities']['location_info']
        try:
            (dest_type, dest_id, dest_pool) = info.split(':')
        except ValueError:
            return None
        if (dest_type != 'InStorageMCSDriver' or dest_id !=
                state['system_id']):
            return None
        return dest_pool

    def add_vdisk_qos(self, vdisk, qos):
        """Add the QoS configuration to the volume."""
        for key, value in qos.items():
            param = self.mcs_qos_keys[key]['param']
            if key in self.mcs_qos_keys:
                if key == 'IOThrottling':
                    self.executor.chvdisk(vdisk, ['-' + param, str(value)])
                if key == 'Bandwidth':
                    self.executor.chvdisk(vdisk, ['-' + param, str(value), '-unitmb'])

    def update_vdisk_qos(self, vdisk, qos):
        """Update all the QoS in terms of a key and value.

        mcs_qos_keys saves all the supported QoS parameters. Going through
        this dict, we set the new values to all the parameters. If QoS is
        available in the QoS configuration, the value is taken from it;
        if not, the value will be set to default.
        """
        for key, value in self.mcs_qos_keys.items():
            param = value['param']
            if key in qos.keys():
                # If the value is set in QoS, take the value from
                # the QoS configuration.
                qos_value = qos[key]
            else:
                # If not, set the value to default.
                qos_value = value['default']
            if key == 'IOThrottling':
                self.executor.chvdisk(vdisk, ['-' + param, str(qos_value)])
            if key == 'Bandwidth':
                self.executor.chvdisk(vdisk, ['-' + param, str(qos_value), '-unitmb'])

    def disable_vdisk_qos(self, vdisk, qos):
        """Disable the QoS."""
        for key, value in qos.items():
            if key in self.mcs_qos_keys:
                param = self.mcs_qos_keys[key]['param']
                # Take the default value.
                value = self.mcs_qos_keys[key]['default']
                if key == 'IOThrottling':
                    self.executor.chvdisk(vdisk, ['-' + param, value])
                if key == 'Bandwidth':
                    self.executor.chvdisk(vdisk, ['-' + param, value, '-unitmb'])

    def change_vdisk_options(self, vdisk, changes, opts):
        if 'warning' in opts:
            opts['warning'] = '%s%%' % str(opts['warning'])
        if 'intier' in opts:
            opts['intier'] = 'on' if opts['intier'] else 'off'
        if 'autoexpand' in opts:
            opts['autoexpand'] = 'on' if opts['autoexpand'] else 'off'
        # can be changed only when pool is SSA
        if 'deduped' in opts:
            opts['deduped'] = 'on' if opts['deduped'] else 'off'
        # can be changed only when pool is SSA
        if 'compressed' in opts:
            opts['compressed'] = 'on' if opts['compressed'] else 'off'

        for key in changes:
            self.executor.chvdisk(vdisk, ['-' + key, opts[key]])

    def change_vdisk_iogrp(self, vdisk, state, iogrp):
        if state['code_level'] < (3, 0, 0, 0):
            LOG.debug('Ignore change IO group as storage code level is '
                      '%(code_level)s, below the required 3, 0, 0, 0.',
                      {'code_level': state['code_level']})
        else:
            self.executor.movevdisk(vdisk, str(iogrp[0]))
            self.executor.addvdiskaccess(vdisk, str(iogrp[0]))
            self.executor.rmvdiskaccess(vdisk, str(iogrp[1]))

    def vdisk_by_uid(self, vdisk_uid):
        """Returns the properties of the vdisk with the specified UID.

        Returns None if no such disk exists.
        """

        vdisks = self.executor.lsvdisks_from_filter('vdisk_UID', vdisk_uid)

        if len(vdisks) == 0:
            return None

        if len(vdisks) != 1:
            msg = (_('Expected single vdisk returned from lsvdisk when '
                     'filtering on vdisk_UID.  %(count)s were returned.') %
                   {'count': len(vdisks)})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        vdisk = vdisks.result[0]

        return self.executor.lsvdisk(vdisk['name'])

    def is_vdisk_in_use(self, vdisk):
        """Returns True if the specified vdisk is mapped to at least 1 host."""
        resp = self.executor.lsvdiskhostmap(vdisk)
        return len(resp) != 0

    def rename_vdisk(self, vdisk, new_name):
        self.executor.chvdisk(vdisk, ['-name', new_name])

    def change_vdisk_primary_copy(self, vdisk, copy_id):
        self.executor.chvdisk(vdisk, ['-primary', copy_id])

    def get_host_info(self, hostname):
        return self.executor.lshost(hostname)

    def migratevdisk(self, vdisk, dest_pool, copy_id='0'):
        self.executor.migratevdisk(vdisk, dest_pool, copy_id)

    def get_vdisk_dependent(self, vdisk):
        return self.executor.lsrmvdiskdependentmaps(vdisk)

    def extend_rc_relationship(self, size, site, rc_name):
        self.executor.expandremotecopysize(size, site, rc_name)
