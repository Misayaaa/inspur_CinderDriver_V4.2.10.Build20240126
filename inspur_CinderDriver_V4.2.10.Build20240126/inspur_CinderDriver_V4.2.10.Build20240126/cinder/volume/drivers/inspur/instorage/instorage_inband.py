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


import copy
import datetime
import random
import re
import threading
import time
import unicodedata

import requests
import six
from eventlet import greenthread
from cinder import context, exception
from cinder import utils as cinder_utils
from cinder.i18n import _
from cinder.volume import qos_specs, volume_types
from cinder.volume.drivers.inspur import constants as instorage_const

# !!!ATTENTION
# the openstack common package is move out from project
# as some independent library from Kilo
try:
    # the config is work as oslo more early
    from cinder.openstack.common import excutils
    from cinder.openstack.common import jsonutils as json
    from cinder.openstack.common import log as logging
    from cinder.openstack.common import strutils, timeutils
except ImportError:
    from oslo_log import log as logging
    from oslo_serialization import jsonutils as json
    from oslo_utils import excutils, strutils, timeutils

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

INTERVAL_1_SEC = 1
DEFAULT_TIMEOUT = 20
LOG = logging.getLogger(__name__)
#_request_semaphore = threading.Semaphore(10)

# Ignore the failed rest response, go through as success.
FAIL_HANDLER_IGNORE = 'ignore'
# Raise exception for the failed rest response.
FAIL_HANDLER_RAISE = 'raise'
# the failed message pattern of rest access, example "CMMEXXXE Failed."
REST_RESPONSE_ERROR_PATTERN = re.compile('CMMVC[0-9]{4}[E|I|V]', re.I)

class RestExecutor(object):
    """inspur InStorage MCS Rest executor for cinder driver."""
    def __init__(self, hostname, port,
                 username, password,
                 ca_root=None, ca_key=None, ca_cert=None,
                 use_ssl=False, check_root_cert=False,
                 check_client_cert=False, host_identifier = -1,
                 thread_num = 10, cluster_id = None):
        self._username = username
        self._password = password
        self._ca_root = ca_root
        self._ca_key = ca_key
        self._ca_cert = ca_cert
        self._use_ssl = use_ssl
        self._check_root_cert = check_root_cert
        self._check_client_cert = check_client_cert
        self._host_identifier = host_identifier
        self._cluster_id = cluster_id
        self._schema = 'https'
        self._baseurl = '%s://%s:%s' % (self._schema, hostname, port)

        self._token = None
        self._token_expire = datetime.datetime.min
        self._request_semaphore = threading.Semaphore(thread_num)
        self._lock = threading.Lock()
        LOG.debug('AS18000 initailize driver with %s %s %s',
                  hostname, port, username)

    @cinder_utils.retry(exception.VolumeDriverException, interval=3, retries=5)
    def login(self):
        """Login the AS18000 and store the token."""
        uri = '/rest/v1/security/token'
        cmd = 'post'
        params = {
            'userName': self._username,
            'password': self._password,
        }

        try:
            _ , data = self._request(cmd, uri, params)
            self._token = data['token']
            self._token_expire = timeutils.utcnow() + datetime.timedelta(
                0, (int(data['expireTime']) - 10))
            LOG.debug('Login AS18000 %s success.', self._baseurl)
        except Exception as exp:
            self._token = None
            self._lock.release()
            LOG.error('Login AS18000 %s failed for %s.', self._baseurl, exp)
            raise exception.VolumeDriverException("AS18000 sys login failed")

    def logout(self):
        """Logout the AS18000 and delete the token."""
        uri = '/rest/v1/security/token'
        cmd = 'delete'
        self._request(cmd, uri)
        LOG.debug('Logout AS18000 %s success.', self._baseurl)

    def is_token_expire(self):
        if self._token is None:
            return True
        return False

    def relogin(self):
        self._lock.acquire()
        if self.is_token_expire():
            self.login()
        self._lock.release()

    @cinder_utils.retry(exception.VolumeDriverException, interval=3, retries=3)
    def request(self, cmd, uri, params=None, fail_handler=None):
        try:
            self.relogin()
            return self._request(cmd, uri, params, fail_handler)
        except exception.VolumeDriverException as exp:
            raise exp

    def _request(self, cmd, uri, params=None, fail_handler=None):
        url = self._baseurl + uri
        http_header = {
            'Content-Type': 'application/json;charset=UTF-8'
        }
        if self._host_identifier != -1:
            http_header.update({'Host-Id': str(self._host_identifier)})
        if self._cluster_id is not None:
            http_header.update({'Cluster-Id': self._cluster_id})

        if (uri != '/rest/v1/security/token' or
               (uri == '/rest/v1/security/token' and cmd == 'delete')):
            http_header.update({
                'X-Auth-Token': self._token,
            })

        header,response = self.do_request(cmd, url, http_header, params)

        code = int(response.get('code'))
        data = None
        if code == 0:
            if 'data' in response:
                data = response.get('data')
            return header, data

        if code in [1,2,3]:
            msg = 'token expired or invaild, need relogin, %s.' % response.get('message')
            self._token = None
            raise exception.VolumeDriverException(msg)

        if code in [4]:
            msg = 'username or password is wrong'
            self._token = None
            raise exception.VolumeDriverException(msg)

        if code == 203:
            # The specified object does not exit.
            return None,None

        if code == 1002 and fail_handler:
            action, data = fail_handler(response)
            if action == FAIL_HANDLER_IGNORE:
                return None,data

        msg = 'access to as18000 failed for %d, the description: %s, the MCS error code: %s' %\
              (code, response.get('description'), response.get('cliError') )
        raise exception.VolumeDriverException(msg)

    def _encode_secret_data(self, param):
        if not isinstance(param, dict):
            return param

        clone = copy.deepcopy(param)

        if 'password' in clone:
            clone['password'] = '******'
        if 'X-Auth-Token' in clone:
            clone['X-Auth-Token'] = '******'
        if 'data' in clone and isinstance(clone['data'], dict):
            if 'token' in clone['data']:
                clone['data']['token'] = '****'
        return clone

    def do_request(self, cmd, url, header, data):
        """Send request to the storage and handle the response."""
        LOG.debug(">>>>>> Rest cmd %s url %s header %s data %s", cmd, url,
                  self._encode_secret_data(header),
                  self._encode_secret_data(data))
        if cmd not in ['post', 'get', 'put', 'delete']:
            msg = (_('Unsupported cmd: %s.') % cmd)
            raise exception.VolumeBackendAPIException(msg)

        data = json.dumps(data) if data else None

        request_params = {
            'data': data,
            'headers': header,
            'verify': False
        }

        if self._use_ssl:
            # check root cert, check whether the ca is reliable
            if self._check_root_cert and self._ca_root:
                request_params.update({
                    'verify': self._ca_root
                })
            else:
                request_params.update({
                    'verify': False
                })
            # check connection cert
            if self._check_client_cert and self._ca_cert and self._ca_key:
                request_params.update({
                    'cert': (self._ca_cert, self._ca_key)
                })

        self._request_semaphore.acquire()
        try:
            start = timeutils.utcnow()
            req = getattr(requests, cmd)(url, **request_params)
            end = timeutils.utcnow()
            interval = end - start
            LOG.debug("<<<<<< Rest request consume<%(interval)s>s cmd: %(cmd)s"
                      " url: %(url)s, params: %(data)s." %
                      {'interval': interval.total_seconds(),
                       'cmd': cmd, 'url': url,
                       'data': self._encode_secret_data(data)})
        except requests.exceptions.SSLError as exp:
            msg = (_('SSL access %(url)s failed for %(exp)s.') %
                   {'url': url, 'exp': exp})
            LOG.error(msg)
            if 'EOF occurred in violation of protocol' in msg:
                raise exception.VolumeDriverException(msg)
            raise exception.VolumeBackendAPIException(msg)
        except requests.exceptions.ConnectionError as ex:
            msg = (_('access %(url)s failed for %(ex)s.') %
                   {'url': url, 'ex': ex})
            LOG.error(msg)
            raise exception.VolumeDriverException(msg)
        except Exception as ex:
            msg = (_('Request access %(url)s failed for %(ex)s.') %
                   {'url': url, 'ex': ex})
            LOG.error(msg)
            raise exception.VolumeDriverException(msg)
        finally:
            self._request_semaphore.release()

        code = req.status_code
        if code != 200:
            msg = (_('Code: %(code)s, URL: %(url)s, Message: %(msg)s.')
                   % {'code': req.status_code,
                      'url': req.url,
                      'msg': req.text})
            LOG.error(msg)
            raise exception.VolumeDriverException(msg)

        response_header = req.headers
        response_body = req.json()
        LOG.debug('CODE: %(code)s, HEADER:%(header)s, RESPONSE: %(response)s.',
                  {'code': code,
                   'header': response_header,
                   'response': self._encode_secret_data(response_body)})

        return response_header,response_body

    def build_iso8601_timestr(self, time):
        return time.isoformat()[:-3] + 'Z'

    def send_rest_api(self, method, params=None, request_type='get',
                      fail_handler=None):
        return self.request(request_type, '/rest/v1/' + method , params,
                            fail_handler)


class InStorageRestAssistant(object):
    """inspur InStorage MCS Rest assistant for cinder driver."""
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

    def __init__(self, run_rest,
                 pool_map=None, iogrp_map=None,
                 loop_check_interval=10):
        self.executor = InStorageRestExector(run_rest)
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
        keys = ['licenseCompressionEnclosures',
                'licenseCompressionCapacity']
        for key in keys:
            if resp.get(key, '0') != '0':
                return True

        resp = self.executor.lsguicapabilities()
        if resp.get('compression', '0') == 'yes':
            return True
        return False

    def deduped_enabled(self):
        """Return whether or not deduplication is enabled for this system."""
        resp = self.executor.lsguicapabilities()
        if resp.get('deduplication', '0') == 'yes':
            return True
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
            capabilities_value = resp.get('licenseTypeLocalcopy', '0')
        except exception.VolumeBackendAPIException:
            LOG.exception(_("Failed to fetch licenseTypeLocalcopy."))
        return capabilities_value

    def replication_licensed(self):
        """Return whether or not replication is enabled for this system."""
        return True

    def get_system_info(self):
        """Return system's name, ID, and code level."""
        resp = self.executor.lssystem()
        level = resp['codeLevel']
        match_obj = re.search('([0-9]+.){3}[0-9]+', level)
        if match_obj is None:
            msg = _('Failed to get code level (%s).') % level
            raise exception.VolumeBackendAPIException(data=msg)
        code_level = match_obj.group().split('.')
        return {'code_level': tuple([int(x) for x in code_level]),
                'system_name': resp['name'],
                'system_id': resp['id'],
                'topology': resp['topology']}

    def get_cluster_capacity_info(self):
        """Return system's total and allocated_extent capacity."""
        resp = self.executor.lssystem()
        return {'total_mdisk_capacity': resp['totalMdiskCapacity'],
                'total_allocated_extent_capacity': resp['totalAllocatedExtentCapacity'],
                'total_vdiskcopy_capacity': resp['totalVdiskcopyCapacity']}

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
                node['IO_group'] = node_data['ioGroupId']
                node['iscsi_name'] = node_data['iscsiName']
                node['site_name'] = node_data['siteName']
                node['WWNN'] = node_data['wwnn']
                node['status'] = node_data['status']
                node['WWPN'] = []
                node['ipv4'] = []
                node['ipv6'] = []
                node['enabled_protocols'] = []
                nodes[node['id']] = node
            except KeyError:
                self.handle_keyerror('lsnode', node_data)
        return nodes

    def is_ssa_pool(self, pool):
        attrs = self.get_pool_attrs(pool)
        try:
            return True if attrs['category'] == 'ssa' else False
        except (AttributeError,KeyError):
            return False

    def get_pool_attrs(self, pool):
        """Return attributes for the specified pool."""
        data = self.executor.lsmdiskgrp(pool)
        pool_data = {}
        pool_data['in_tier'] = data['in_tier'] if 'in_tier' in data else data['inTier']
        pool_data['capacity'] = data['capacity']
        pool_data['free_capacity'] = (data['free_capacity'] if 'free_capacity' in data
                                          else data['freeCapacity'])
        pool_data['used_capacity'] = (data['used_capacity'] if 'used_capacity' in data
                                          else data['usedCapacity'])
        pool_data['virtual_capacity'] = (data['virtual_capacity'] if 'virtual_capacity' in data
                                            else data['virtualCapacity'])
        pool_data['name'] = data['name']
        pool_data['category'] = data['category'] if 'category' in data else 'common'
        if 'snapshotUsedCapacity' in data:
            pool_data['snapshot_used_capacity'] = data['snapshotUsedCapacity']
        if 'nonsnapshotVritualCapacity' in data:
            pool_data['nonsnapshot_virtual_capacity'] = data['nonsnapshotVritualCapacity']

        return pool_data

    def get_available_io_groups(self):
        """Return list of available IO groups."""
        iogrps = []
        resp = self.executor.lsiogrp()
        for iogrp in resp:
            try:
                if int(iogrp['nodeCount']) > 0:
                    iogrps.append(int(iogrp['id']))
            except KeyError:
                self.handle_keyerror('lsiogrp', iogrp)
            except ValueError:
                msg = (_('Expected integer for node_count, '
                         'mcsinq lsiogrp returned: %(node)s.') %
                       {'node': iogrp['nodeCount']})
                raise exception.VolumeBackendAPIException(data=msg)
        return iogrps

    def get_vdisk_count_by_io_group(self):
        res = {}
        resp = self.executor.lsiogrp()
        for iogrp in resp:
            try:
                if int(iogrp['nodeCount']) > 0:
                    res[int(iogrp['id'])] = int(iogrp['vdiskCount'])
            except KeyError:
                self.handle_keyerror('lsiogrp', iogrp)
            except ValueError:
                msg = (_('Expected integer for node_count, '
                         'mcsinq lsiogrp returned: %(node)s') %
                       {'node': iogrp['nodeCount']})
                raise exception.VolumeBackendAPIException(data=msg)
        return res

    def select_io_group(self, state, opts):
        selected_iog = 0
        iog_list = InStorageRestAssistant.get_valid_requested_io_groups(
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
                if iogrp['name'] == vdisk['ioGroupName']:
                    return int(iogrp['id'])
        return None

    def add_ip_addrs(self, storage_nodes):
        """Add IP addresses to system node information."""
        resp = self.executor.lsportip()
        filter_ips_v4 = ['172.16.2.33','172.16.2.43','172.16.2.53',
                         '172.16.2.32','172.16.2.42','172.16.2.52']
        for ip_data in resp:
            try:
                state = ip_data['state']
                if ip_data['nodeId'] in storage_nodes and (
                        state == 'configured' or state == 'online'):
                    node = storage_nodes[ip_data['nodeId']]
                    if (len(ip_data['ipAddress'])
                            and ip_data['ipAddress'] not in filter_ips_v4):
                        node['ipv4'].append(ip_data['ipAddress'])
                    if len(ip_data['ipAddress6']):
                        node['ipv6'].append(ip_data['ipAddress6'])
            except KeyError:
                self.handle_keyerror('lsportip', ip_data)
                resp = self.executor.lsportip()

    def add_fc_wwpns(self, storage_nodes):
        """Add FC WWPNs to system node information."""
        for key in storage_nodes:
            node = storage_nodes[key]
            wwpns = set(node['WWPN'])
            resp = self.executor.lsportfc(node_id=node['id'])
            for port_info in resp:
                if (port_info['type'] == 'fc' and
                        port_info['status'] == 'active'):
                    wwpns.add(port_info['wwpn'])
            node['WWPN'] = list(wwpns)
            LOG.info(_('WWPN on node %(node)s: %(wwpn)s.'),
                     {'node': node['id'], 'wwpn': node['WWPN']})

    def get_conn_fc_wwpns(self, host):
        wwpns = set()
        resp = self.executor.lsfabric(host=host)
        local_wwpn_list = []
        for fabric_info in resp:
            local_wwpn_list.append(fabric_info['localWwpn'])
        for wwpn in local_wwpn_list:
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
                    if host_data['iscsiAuthMethod'] == 'chap':
                        return host_data['iscsiChapSecret']
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
                        if (wwpn_info['remoteWwpn'] and
                                wwpn_info['name'] and
                                wwpn_info['remoteWwpn'].lower() ==
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
        host_list = []
        for host_info in hosts_info:
            host_list.append(host_info['name'])
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
            hosts_map_info_list = []
            for host_map_info in hosts_map_info:
                hosts_map_info_list.append(host_map_info['hostName'])
            # remove the fast path host names from the end of the list
            # and move to the front so they are only searched for once.
            for host in hosts_map_info_list:
                update_host_list(host, host_list)
        found = False
        for name in host_list:

            resp = self.executor.lshost(host=name)
            if not resp:
                LOG.warning("host %s is not exists " % name)
                continue
            if 'initiator' in connector:
                if 'iscsiName' in resp:
                    iscsi_name_list = list(resp['iscsiName'])
                    for iscsi in iscsi_name_list:
                        if iscsi == connector['initiator']:
                            host_name = name
                            found = True
                            break
            elif 'wwpns' in connector and len(connector['wwpns']):
                if 'wwpn' in resp:
                    connector_wwpns = [str(x).lower() for x in connector['wwpns']]
                    wwpn_list = list(resp['wwpn'])
                    for wwpn in wwpn_list:
                        if wwpn and wwpn.lower() in connector_wwpns:
                            host_name = name
                            found = True
                            break
            elif 'nqn' in connector:
                if 'nqn' in resp and resp['nqn'] == connector['nqn']:
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
        if 'wwpns' in connector:
            for wwpn in connector['wwpns']:
                ports.append(['wwpn', '%s' % wwpn])
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
        self.executor.mkhost(host_name, port[0], port[1], site_name)

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

    def map_vol_to_host(self, volume_name, host_name, multihostmap):
        """Create a mapping between a volume to a host."""

        LOG.debug('Enter: map_vol_to_host: volume %(volume_name)s to '
                  'host %(host_name)s.',
                  {'volume_name': volume_name, 'host_name': host_name})

        # Check if this volume is already mapped to this host
        iogrp_lun_id_map = self.executor.get_vdiskhostmapid(volume_name, host_name)
        if len(iogrp_lun_id_map) == 0:
            iogrp_lun_id_map = self.executor.mkvdiskhostmap(host_name, volume_name, None,
                                                 multihostmap)

        LOG.debug('Leave: map_vol_to_host: iogrp_lun_id_map %(iogrp_lun_id_map)s, volume '
                  '%(volume_name)s, host %(host_name)s.',
                  {'iogrp_lun_id_map': iogrp_lun_id_map,
                   'volume_name': volume_name,
                   'host_name': host_name})
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
                host_name = resp[0]['hostName']
        else:
            found = False
            host_name_list = []
            for vdisk_host_map_info in resp:
                host_name_list.append(vdisk_host_map_info['hostName'])
            for value in host_name_list:
                if value == host_name:
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

        iogs = InStorageRestAssistant.get_valid_requested_io_groups(state, opts)

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
                if key in ['replication','compressed', 'deduped']:
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
        params = {}
        params.update({'syncrate': opts['syncrate']})
        if is_ssa_pool or (opts['rsize'] != -1 and opts['rsize'] != 100):
            if opts['rsize'] == -1 or opts['rsize'] == 100:
                opts['rsize'] = 2
            params.update({'rsize': '%s%%' % str(opts['rsize'])})
            if opts['autoexpand']:
                params.update({'autoexpand':'yes'})
            params.update({'warning': '%s%%' % str(opts['warning'])})

        if is_ssa_pool:
            params.update({'grainsize': int(opts['grainsize'])})
            if opts['deduped']:
                params.update({'deduped': 'true'})
            if opts['compressed']:
                params.update({'compressed': 'true'})
        elif opts['rsize'] != -1 and opts['rsize'] != 100:
            if opts['compressed']:
                params.update({'compressed': 'true'})
            else:
                params.update({'grainsize': int(opts['grainsize'])})
        # intier option is ignore when pool is ssa
        if not is_ssa_pool:
            intier = 'on' if opts['intier'] else 'off'
            params.update({'intier': intier})
        return params

    @staticmethod
    def _get_volume_create_params(opts, is_ssa_pool=False):
        params = {}
        if is_ssa_pool or (opts['rsize'] != -1 and opts['rsize'] != 100):
            params.update({
                'buffersize': '%s%%' % str(opts['rsize']),
                'warning': '%s%%' % str(opts['warning']),
            })
            if not opts['autoexpand']:
                params.update({
                    'noautoexpand': 'true',
                })

        if is_ssa_pool:
            params.update({
                'thin': 'true',
                'grainsize': str(opts['grainsize'])
                })
            if opts['deduped']:
                params.update({'deduped': 'true'})
            if opts['compressed']:
                params.update({'compressed': 'true'})
        elif opts['rsize'] != -1 and opts['rsize'] != 100:
            if opts['compressed']:
                params.update({'compressed': 'true'})
            else:
                params.update({
                    'thin': 'true',
                    'grainsize': str(opts['grainsize'])
                })
        # thick volume has no other params
        return params

    def check_ssa_pool_params(self, opts):
        """Check the configured parameters if vol in ssa pool."""
        if opts['rsize'] == -1 or opts['rsize'] == 100:
            opts['rsize'] = 2
        if opts['compressed'] or opts['deduped']:
            opts['grainsize'] = 8
        elif opts['grainsize'] not in [8, 32]:
            opts['grainsize'] = 8

    def check_normal_pool_params(self, opts):
        """Check the configured parameters if vol in normal pool."""
        if opts['deduped']:
            msg = (_('You cannot create deduped volume in normal pool'))
            raise exception.VolumeDriverException(message=msg)
        if opts['compressed']:
            if opts['rsize'] == -1 or opts['rsize'] == 100:
                opts['rsize'] = 2

    def create_vdisk(self, name, size, units, pool, opts, nofmtdisk=False):
        LOG.debug('we will create a general volume %s' % name)

        name = '%s' % name
        is_ssa_pool = self.is_ssa_pool(pool)
        if is_ssa_pool:
            self.check_ssa_pool_params(opts)
        else:
            self.check_normal_pool_params(opts)
        params = self._get_vdisk_create_params(opts, is_ssa_pool)
        self.executor.mkvdisk(name, size, units, pool, opts, params, nofmtdisk)

    def create_volume(self, name, size, units, pool, opts):
        LOG.debug('we will create an active-active volume %s' % name)
        name = '%s' % name
        is_ssa_pool = self.is_ssa_pool(pool)
        if is_ssa_pool:
            self.check_ssa_pool_params(opts)
        else:
            self.check_normal_pool_params(opts)
        params = self._get_volume_create_params(opts, is_ssa_pool)
        poollist = [pool,self.pool_map[pool]]
        iogrp = '%s' % opts['iogrp']
        iogrplist = '%s:%s' % (iogrp, self.iogrp_map[iogrp])
        self.executor.mkvolume(name, size, units, poollist, iogrplist, params)

    def transfer_vdisk_to_volume(self, name, pool, opts):
        LOG.debug('we will transfter vdisk %s to active-active volume.' % name)

        name = '%s' % name
        params = self._get_volume_create_params(opts, self.is_ssa_pool(pool))
        dst_pool = self.pool_map[pool]
        dst_iogrp = self.iogrp_map[opts['iogrp']]
        self.executor.addvolumecopy(name, dst_pool, dst_iogrp, params)

    def transfer_volume_to_vdisk(self, name, pool):
        LOG.debug('we will transfer volume %s to general vdisk by delete a copy.' % name)

        name = '%s' % name
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
            self.ensure_vdisk_no_lc_mappings(vdisk, allow_snaps=False,
                                             allow_lctgt=True)

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
                'mdisk_grp_name': attr['mdiskGrpName'],
                'IO_group_id': attr['IOGroupId']
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
            iogrps.add(attr['ioGroupId'])
        return iogrps

    def get_volume_prefer_site_name(self, volume_name):
        site_name = ''
        prefer_iogrp_name = None

        attrs = self.executor.lsvdisks_from_filter('volume_name', volume_name)
        topology = self.get_system_info()['topology']
        for attr in attrs:
            if attr['function'] == 'master':
                prefer_iogrp_name = attr['ioGroupName']
                break
            if topology  == 'inmetro':
                prefer_iogrp_name = attr['ioGroupName']
                break
        if prefer_iogrp_name is None:
            return site_name

        iogrps = self.executor.lsiogrp()
        for iogrp in iogrps:
            if iogrp['name'] == prefer_iogrp_name:
                site_name = iogrp['siteName']
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
                for vdiskcopy in resp:
                    vdisk_name = vdiskcopy['vdiskName']
                    used_capacity = vdiskcopy['usedCapacity']
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
        return self.executor.lsvdiskcopy(vdisk, copy_id=copy_id)

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
                progress_info = self.executor.lsvdisksyncprogress(vdisk, copy_id)
                copy['sync_progress'] = progress_info['progress']
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
        src_node = src_attrs['preferredNodeId']
        if src_node and int(src_node) != 0:
            opts['node'] = src_node
        pool = src_attrs['mdiskGrpName']
        opts['iogrp'] = src_attrs['ioGroupId']
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
        orig_copy_id = resp[0].get("copyId", None)

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
        is_ssa_pool = self.is_ssa_pool(dest_pool)
        if is_ssa_pool:
            self.check_ssa_pool_params(opts)
        else:
            self.check_normal_pool_params(opts)
        params = self._get_vdisk_create_params(opts, is_ssa_pool)
        new_copy_id = self.executor.addvdiskcopy(vdisk, dest_pool, params)
        return (orig_copy_id, new_copy_id)

    def check_vdisk_copy_synced(self, vdisk, copy_id):
        sync = self.executor.lsvdiskcopy(vdisk, copy_id=copy_id)['sync']
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

    def stop_lcmap(self,lc_map_id,split=False):
        self.executor.stoplcmap(lc_map_id, split)

    def rm_lcmap(self,lc_map_id):
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
                 'status': model_update['status']})

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

        lc_map_id = self.executor.mklcmap(source, target, full_copy, copy_rate, clean_rate)
        #self._prepare_lc_map(lc_map_id, timeout, restore)
        self.executor.startlcmap(lc_map_id, restore)

        LOG.debug('Leave: run_localcopy: LocalCopy started from '
                  '%(source)s to %(target)s.',
                  {'source': source, 'target': target})

    def create_localcopy_to_consistgrp(self, source, target, consistgrp,
                                       opts, full_copy=False,pool=None):
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
        pool = src_attrs['mdiskGrpName']
        opts['iogrp'] = src_attrs['ioGroupId']
        src_node = src_attrs['preferredNodeId']
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
            if attrs['sourceVdiskName'] == vdisk and type in ['all', 'source']:
                src_mappings.append(attrs)
            elif attrs['targetVdiskName'] == vdisk and type in ['all', 'target']:
                tgt_mappings.append(attrs)

        return src_mappings, tgt_mappings

    def get_localcopy_mapping_attributes(self, lc_map_id):
        resp = self.executor.lslcmap(lc_map_id)
        if not resp:
            return None
        return resp

    def _get_localcopy_consistgrp_attr(self, lc_map_id):
        resp = self.executor.lslcconsistgrp(lc_map_id)
        return resp

    def _check_vdisk_lc_mappings(self, name,
                                 allow_snaps=True, allow_lctgt=False, type='all'):
        """LocalCopy mapping check helper."""
        LOG.debug('Loopcall: _check_vdisk_lc_mappings(), vdisk %s.', name)
        wait_for_copy = False
        rmlcmap_failed_e = None
        src_mappings, tgt_mappings = self.get_vdisk_lc_mappings(name, type)
        for mappings in [src_mappings, tgt_mappings]:
            for attrs in mappings:
                map_id = attrs['id']
                source = attrs['sourceVdiskName']
                target = attrs['targetVdiskName']
                copy_rate = attrs['copyRate']
                status = attrs['status']
                progress = attrs['progress']

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
            source = attrs['sourceVdiskName']
            target = attrs['targetVdiskName']
            copy_rate = attrs['copyRate']
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
        if vol_attrs['rcName']:
            self.executor.startrcrelationship(vol_attrs['rcName'], primary)

    def stop_relationship(self, volume_name, access=False):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if vol_attrs['rcName']:
            self.executor.stoprcrelationship(vol_attrs['rcName'], access=access)

    def create_relationship(self, master, aux, system, asynccopy):
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

    def delete_relationship(self, volume_name):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if vol_attrs['rcName']:
            self.executor.rmrcrelationship(vol_attrs['rcName'], True)

    def get_relationship_info(self, volume_name):
        vol_attrs = self.get_vdisk_attributes(volume_name)
        if not vol_attrs or not vol_attrs['rcName']:
            LOG.debug(_("Unable to get remote copy information for "
                       "volume %s"), volume_name)
            return None

        relationship = self.executor.lsrcrelationship(vol_attrs['rcName'])
        return relationship if len(relationship) > 0 else None

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
        return partnership if len(partnership) > 0 else None

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
            if key in self.mcs_qos_keys:
                if key == 'IOThrottling':
                    param = self.mcs_qos_keys[key]['param']
                    params = {param:int(value)}
                    self.executor.chvdisk(vdisk, params)
                if key == 'Bandwidth':
                    param = self.mcs_qos_keys[key]['param']
                    params = {param:int(value),
                             'unitmb':'true'
                             }
                    self.executor.chvdisk(vdisk, params)

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
                params = {param: int(qos_value)}
                self.executor.chvdisk(vdisk, params)
            if key == 'Bandwidth':
                params = {param:int(qos_value),
                        'unitmb':'true'
                         }
                self.executor.chvdisk(vdisk, params)

    def disable_vdisk_qos(self, vdisk, qos):
        """Disable the QoS."""
        for key, value in qos.items():
            if key in self.mcs_qos_keys:
                param = self.mcs_qos_keys[key]['param']
                # Take the default value.
                value = self.mcs_qos_keys[key]['default']
                if key == 'IOThrottling':
                    params = {param: value}
                    self.executor.chvdisk(vdisk, params)
                if key == 'Bandwidth':
                    params = {param:int(value),
                             'unitmb':'true'
                             }
                    self.executor.chvdisk(vdisk, params)

    def change_vdisk_options(self, vdisk, changes, opts):
        params = {}
        if 'warning' in opts and 'warning' in changes:
            params.update({
                'warning': '%s%%' % str(opts['warning'])
            })
        if 'intier' in opts and 'intier' in changes:
            params.update({
                'intier': 'on' if opts['intier'] else 'off'
            })
        if 'autoexpand' in opts and 'autoexpand' in changes:
            params.update({
                'autoexpand': 'on' if opts['autoexpand'] else 'off'
            })
        if 'deduped' in opts:
            params.update({
                'deduped': 'on' if opts['deduped'] else 'off'
            })
        # can be changed only when pool is SSA
        if 'compressed' in opts:
            params.update({
                'compressed': 'on' if opts['compressed'] else 'off'
            })
        if len(params):
            self.executor.chvdisk(vdisk, params)

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

        vdisk = self.executor.lsvdisks_from_filter('vdisk_UID', vdisk_uid)
        return self.executor.lsvdisk(vdisk['name'])

    def is_vdisk_in_use(self, vdisk):
        """Returns True if the specified vdisk is mapped to at least 1 host."""
        resp = self.executor.lsvdiskhostmap(vdisk)
        return len(resp) != 0

    def rename_vdisk(self, vdisk, new_name):
        params = {
                'name': new_name
            }
        self.executor.chvdisk(vdisk, params)

    def change_vdisk_primary_copy(self, vdisk, copy_id):
        params = {
                'primary': copy_id
            }
        self.executor.chvdisk(vdisk, params)

    def clear_scsi_cache(self):
        self.executor.clearmcscache()

    def get_host_info(self, hostname):
        return self.executor.lshost(hostname)

    def migratevdisk(self, vdisk, dest_pool, copy_id='0'):
        self.executor.migratevdisk(vdisk, dest_pool, copy_id)


class InStorageRestExector(object):
    """Rest Exector interface to inspur InStorage systems."""

    def __init__(self, run_rest):
        self._executor = run_rest

    @cinder_utils.retry(exception.VolumeDriverException, interval=5, retries=3, backoff_rate=1)
    def _run_rest(self, method, params=None, request_type='get', fail_handler=None):
        return self._executor(method=method, params=params,
                           request_type=request_type, fail_handler=fail_handler)

    def lsnode(self, node_id=None):
        if node_id:
            method = ('cluster/node/%s' % (node_id))
        else:
            method = 'cluster/node'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lslicense(self):
        method = 'system/license'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsguicapabilities(self):
        method = 'cluster/guicapabilities'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lssystem(self):
        method = 'cluster/system'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsmdiskgrp(self, pool):
        method = ('block/mdiskgrp/%s' % (pool))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lsiogrp(self):
        method = 'cluster/iogrp'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsportip(self):
        method = 'device/portip'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lshost(self, host=None):
        method = 'block/host'
        if host:
            method = ('block/host/%s' % (host))
        request_type = 'get'
        def lshost_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5754E: host not exist.
            if matched and matched.group(0) in ['CMMVC5754E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        _, data = self._run_rest(
            method=method, request_type=request_type,
            fail_handler=lshost_fail_handler
        )
        return data

    def lsiscsiauth(self):
        method = 'security/iscsiauth'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsfabric(self, wwpn=None, host=None):
        if wwpn:
            method = ('device/fabric?wwpn=%s' % (wwpn))
        elif host:
            method = ('device/fabric?host=%s' % (host))
        else:
            method = 'device/fabric'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsrcrelationship(self, rc_rel):
        method = ('dataprotection/rcrelationship?filtervalue=name%%3d%s' % (rc_rel))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lspartnership(self, system_name):
        method = ('cluster/partnership?filtervalue=name%%3d%s' % (system_name))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lspartnershipcandidate(self):
        method = 'cluster/partnershipcandidate'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lsvdiskhostmap(self, vdisk):
        method = ('block/vdiskhostmap/%s' % (vdisk))
        request_type = 'get'
        def lsvdiskhostmap_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5754E: mapping not exist.
            if matched and matched.group(0) in ['CMMVC5754E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        _, data = self._run_rest(
            method=method, request_type=request_type,
            fail_handler=lsvdiskhostmap_fail_handler
        )
        return data

    def lshostvdiskmap(self, host):
        method = ('block/hostvdiskmap/%s' % (host))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lsvdisk(self, vdisk):
        """Return vdisk attributes or None if it doesn't exist."""
        method = ('block/vdisk/%s' % (vdisk))
        request_type = 'get'
        def lsvdisk_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5754E: volume not exist.
            if matched and matched.group(0) in ['CMMVC5754E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        _, data = self._run_rest(
            method=method, request_type=request_type, fail_handler=lsvdisk_fail_handler
        )
        return data

    def lsvdisks_from_filter(self, filter_name, value):
        """Performs an lsvdisk command, filtering the results as specified.

        Returns an iterable for all matching vdisks.
        """
        method = ('block/vdisk?filtervalue=%s%%3d%s' % (filter_name,value))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lsvdisklcmappings(self, vdisk):
        method = ('dataprotection/vdisklcmappings/%s' % (vdisk))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type,
        )
        return data

    def lslcmap(self, lc_map_id):
        method = ('dataprotection/lcmap/%s' % (lc_map_id))
        request_type = 'get'
        def lslcmap_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5753E: lcmap not exist.
            if matched and matched.group(0) in ['CMMVC5753E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        _, data = self._run_rest(
            method=method, request_type=request_type,
            fail_handler=lslcmap_fail_handler
        )
        return data

    def lslcconsistgrp(self, lc_consistgrp):
        method = ('dataprotection/lcconsistgrp/%s' % (lc_consistgrp))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsvdiskcopy(self, vdisk=None, copy_id=None, pool=None):
        request_type = 'get'
        if vdisk:
            method = ('dataprotection/vdiskcopy/%s' % (vdisk))
        if copy_id:
            method = ('dataprotection/vdiskcopy/%s?copy=%d' % (vdisk, int(copy_id)))
        if pool:
            method = ('dataprotection/vdiskcopy?filtervalue=mdisk_grp_name%%3d%s' 
                      % (pool))
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsvdisksyncprogress(self, vdisk, copy_id):
        method = ('dataprotection/vdisksyncprogress/%s?copy=%s' % (vdisk,copy_id))
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    def lsportfc(self, node_id):
        if node_id:
            method = ('device/portfc?filtervalue=node_id%%3d%s' % (node_id))
        else:
            method = 'device/portfc'
        request_type = 'get'
        _, data = self._run_rest(
            method=method, request_type=request_type
        )
        return data

    @staticmethod
    def _create_port_arg(port_type, port_name):
        port_param = {}
        if port_type == 'initiator':
            port_param = {
                'iscsiname': [port_name]
            }
        else:
            port_param = {
                'hbawwpn': [port_name]
            }
        return port_param

    def mkhost(self, host_name, port_type, port_name, site=''):
        params = {}
        port_param = self._create_port_arg(port_type, port_name)
        method = 'block/host'
        request_type = 'post'
        params.update({
            'force': 'yes',
            'name' : host_name,
        })
        params.update(port_param)
        if site:
            params.update({
                'site': site
            })
        _, data = self._run_rest(
            method=method, request_type=request_type, params=params
        )
        return data

    def addhostport(self, host, port_type, port_name):
        params = {'force': 'yes'}
        port_param = self._create_port_arg(port_type, port_name)
        params.update(port_param)
        method = ('block/hostport/%s' % (host))
        request_type = 'post'
        _, data = self._run_rest(
            method=method, request_type=request_type, params=params
        )
        return data

    def add_chap_secret(self, secret, host):
        params = {'chapsecret': secret}
        method = ('block/host/%s' % (host))
        request_type = 'put'
        _, data = self._run_rest(
            method=method, request_type=request_type, params=params
        )
        return data

    def mkvdiskhostmap(self, host, vdisk, lun, multihostmap):
        """Map vdisk to host.

        If vdisk already mapped and multihostmap is True, use the force flag.
        """
        params = {'host': host}
        method = ('block/vdiskhostmap/%s' % (vdisk))
        request_type = 'post'
        if lun:
            params.update({
                'scsi': lun
            })
        if multihostmap:
            params.update({
                'force': 'yes'
            })
        self._run_rest(
            method=method, request_type=request_type, params=params
        )
        iogrp_lun_id_map = self.get_vdiskhostmapid(vdisk, host)
        if not iogrp_lun_id_map:
            msg = (_('the vdisk %(vdisk)s map to the host %(host)s failed') %
                    {'vdisk': vdisk,
                    'host': host})
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)
        return iogrp_lun_id_map

    def mkrcrelationship(self, master, aux, system, asynccopy):
        method = 'dataprotection/rcrelationship'
        params = {'master': master,
                  'aux': aux,
                  'cluster': system}
        if asynccopy:
            params.update({
                'async': 'yes'
            })
        request_type = 'post'
        header, _ = self._run_rest(
            method=method, request_type=request_type, params=params
        )
        if 'created-object-id' in header:
            return header['created-object-id']
        return None

    def rmrcrelationship(self, relationship, force=False):
        method = ('dataprotection/rcrelationship/%s' % (relationship))
        params = {}
        if force:
            params.update({
                'force': 'yes'
            })
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def switchrelationship(self, relationship, aux=True):
        method = ('dataprotection/switchrcrelationship/%s' % (relationship))
        params = {}
        if aux:
            params.update({
                'primary': 'aux'
            })
        else:
            params.update({
                'primary': 'master'
            })
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def startrcrelationship(self, rc_rel, primary=None):
        method = ('dataprotection/startrcrelationship/%s' % (rc_rel))
        params = {'force': 'yes'}
        if primary:
            params.update({
                'primary': 'master'
            })
        else:
            params.update({
                'primary': 'aux'
            })
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def stoprcrelationship(self, relationship, access=False):
        method = ('dataprotection/stoprcrelationship/%s' % (relationship))
        params = {}
        if access:
            params.update({
                'access': 'yes'
            })
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def mkippartnership(self, ip_v4, bandwith=1000, backgroundcopyrate=50):
        params = {'type': 'ipv4',
                  'clusterip': ip_v4,
                  'linkbandwidthmbits': int(bandwith),
                  'backgroundcopyrate': int(backgroundcopyrate)}
        method = 'dataprotection/ippartnership'
        request_type = 'post'
        self._run_rest(method=method, request_type=request_type, params=params)

    def mkfcpartnership(self, system_name, bandwith=1000,
                        backgroundcopyrate=50):
        params = {'linkbandwidthmbits': int(bandwith),
                  'backgroundcopyrate': int(backgroundcopyrate)}
        method = ('dataprotection/fcpartnership/%s' % (system_name))
        request_type = 'post'
        self._run_rest(method=method, request_type=request_type, params=params)

    def chpartnership(self, partnership_id, start=True):
        method = ('cluster/partnership/%s' % (partnership_id))
        params = {}
        if start:
            params.update({'start':'yes'})
        else:
            params.update({'stop':'yes'})
        request_type = 'put'
        self._run_rest(method=method, request_type=request_type, params=params)

    def rmvdiskhostmap(self, host, vdisk):
        method = ('block/vdiskhostmap/%s?host=%s' % (vdisk,host))
        request_type = 'delete'
        def rmvdiskhostmap_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5842E: map not exist.
            if matched and matched.group(0) in ['CMMVC5842E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        self._run_rest(
            method=method, request_type=request_type,
            fail_handler=rmvdiskhostmap_fail_handler
        )

    def get_vdiskhostmapid(self, vdisk, host):
        # return the map that the key is io_group_id and the value is lun_id
        result_map = {}
        resp = self.lsvdiskhostmap(vdisk)
        for mapping_info in resp:
            if mapping_info['hostName'] == host:
                result_map[mapping_info['ioGroupId']] = int(mapping_info['scsiId'])
        return result_map

    def rmhost(self, host):
        method = ('block/host/%s' % (host))
        request_type = 'delete'
        def rmhost_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5753E: host not exist.
            if matched and matched.group(0) in ['CMMVC5753E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        self._run_rest(
            method=method, request_type=request_type, fail_handler=rmhost_fail_handler
        )

    def mkvolume(self, name, size, units, poollist, ipgrplist, params):
        method = 'block/volume'
        request_type = 'post'
        params.update({
            'name': name,
            'size': size,
            'unit': units,
            'pool': poollist
        })
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def mkvdisk(self, name, size, units, pool, opts, params,
                nofmtdisk=False):
        method = 'block/vdisk'
        request_type = 'post'
        params.update({
            'name': name,
            'size': size,
            'unit': units,
            'mdiskgrp': [pool],
            'iogrp': six.text_type(opts['iogrp'])
        })
        if nofmtdisk:
            params.update({'nofmtdisk':'yes'})
        if opts['node']:
            params.update({'node': '%s' % opts['node']})
        if opts['access_iogrp']:
            iogrp_list = opts['access_iogrp'].split(':')
            params.update({'accessiogrp': iogrp_list})

        def mkvdisk_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC6035E: volume already exist.
            if matched and matched.group(0) in ['CMMVC6035E','CMMVC6372W']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None

        self._run_rest(
            method=method, request_type=request_type, params=params,
            fail_handler=mkvdisk_fail_handler
        )

    def rmvolume(self, vdisk, force=True):
        method = ('block/volume/%s' % (vdisk))
        if force:
            method = ('block/volume/%s?removehostmappings=yes&removercrelationships=yes&' \
            'removelcmaps=yes&discardimage=yes&cancelbackup=yes' % (vdisk))
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type
        )

    def rmvdisk(self, vdisk, force=True):
        method = ('block/vdisk/%s' % (vdisk))
        if force:
            method = ('block/vdisk/%s?force=yes' % (vdisk))
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type
        )

    def chvdisk(self, vdisk, params):
        method = ('block/vdisk/%s' % (vdisk))
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def movevdisk(self, vdisk, iogrp):
        method = ('block/movevdisk/%s' % (vdisk))
        request_type = 'put'
        params = {'iogrp': iogrp}
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def expandvdisksize(self, vdisk, amount):
        method = ('block/expandvdisksize/%s' % (vdisk))
        params={
            'size': amount,
            'unit': 'gb'
        }
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def mklcmap(self, source, target, full_copy, copy_rate, clean_rate, consistgrp=None):
        method = 'dataprotection/lcmap'
        request_type = 'post'
        params = {
            'source': source,
            'target': target
        }
        if not full_copy:
            params.update({'copyrate': 0})
        else:
            params.update({'copyrate': int(copy_rate)})
            params.update({'cleanrate': int(clean_rate)})
            params.update({'autodelete': 'yes'})
        if consistgrp:
            params.update({'consistgrp': consistgrp})
        header, _ = self._run_rest(
            method=method, request_type=request_type, params=params
        )
        # how to get lcmapid
        if 'created-object-id' in header:
            return header['created-object-id']
        return None

    def prestartlcmap(self, lc_map_id, restore=False):
        method = ('dataprotection/prestartlcmap/%s' % (lc_map_id))
        params = None
        if restore:
            params = {'restore':'true'}
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def startlcmap(self, lc_map_id, restore=False):
        method = ('dataprotection/startlcmap/%s' % (lc_map_id))
        params = {'prep':'true'}
        if restore:
            params.update({'restore': 'true'})
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def prestartlcconsistgrp(self, lc_consist_group):
        method = ('dataprotection/prestartlcconsistgrp/%s' % (lc_consist_group))
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type
        )

    def startlcconsistgrp(self, lc_consist_group):
        method = ('dataprotection/startlcconsistgrp/%s' % (lc_consist_group))
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type
        )

    def stoplcconsistgrp(self, lc_consist_group):
        method = ('dataprotection/stoplcconsistgrp/%s' % (lc_consist_group))
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type
        )

    def chlcmap(self, lc_map_id, copyrate='50', autodel='on'):
        method = ('dataprotection/lcmap/%s' % (lc_map_id))
        params = {
            'copyrate': int(copyrate),
            'autodelete': autodel
        }
        request_type = 'put'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def stoplcmap(self, lc_map_id, split=False):
        method = ('dataprotection/stoplcmap/%s' % (lc_map_id))
        params = None
        if split:
            params = {'split':'true'}
        request_type = 'put'
        def stoplcmap_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5753E: lcmap not exist.
            if matched and matched.group(0) in ['CMMVC5753E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None

        self._run_rest(
            method=method, request_type=request_type, params=params,
            fail_handler=stoplcmap_fail_handler
        )

    @cinder_utils.retry(exception.VolumeDriverException, interval=5, retries=60, backoff_rate=1)
    def rmlcmap(self, lc_map_id):
        method = ('dataprotection/lcmap/%s?force=yes' % (lc_map_id))
        request_type = 'delete'
        def rmlcmap_fail_handler(response):
            message = response.get('cliError', '')
            matched = REST_RESPONSE_ERROR_PATTERN.match(message)
            # CMMVC5753E: lcmap not exist.
            if matched and matched.group(0) in ['CMMVC5753E']:
                return FAIL_HANDLER_IGNORE, None
            return FAIL_HANDLER_RAISE, None
        self._run_rest(
            method=method, request_type=request_type, fail_handler=rmlcmap_fail_handler
        )

    def mklcconsistgrp(self, lc_consist_group, fast):
        method = 'dataprotection/lcconsistgrp'
        request_type = 'post'
        params = {
            'name': lc_consist_group
        }
        if fast:
            params.update({'fast': 'yes'})
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def rmlcconsistgrp(self, lc_consist_group):
        method = ('dataprotection/lcconsistgrp/%s?force=yes' % (lc_consist_group))
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type
        )

    def addvdiskcopy(self, vdisk, dest_pool, params):
        method = ('dataprotection/vdiskcopy/%s' % (vdisk))
        params.update({
            'mdiskgrp': [dest_pool]
        })
        request_type = 'post'
        header, _ = self._run_rest(
            method=method, request_type=request_type, params=params
        )
        if 'created-copy-id' in header:
            return header['created-copy-id']
        return None

    def rmvdiskcopy(self, vdisk, copy_id):
        method = ('dataprotection/vdiskcopy/%s?copy=%d' % (vdisk, int(copy_id)))
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type
        )

    def addvolumecopy(self, vdisk_name, dest_pool, dest_iogrp, params):
        method = ('dataprotection/volumecopy/%s' % (vdisk_name))
        params.update({
            'iogrp': '"%s"' % dest_iogrp,
            'pool': '"%s"' % dest_pool
        })
        request_type = 'post'
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def rmvolumecopy(self, name, pool):
        method = ('dataprotection/volumecopy/%s?pool=%s' % (name, pool))
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type
        )

    def addvdiskaccess(self, vdisk, iogrp):
        method = ('block/vdiskaccess/%s' % (vdisk))
        request_type = 'post'
        params = {'iogrp': iogrp}
        self._run_rest(
            method=method, request_type=request_type, params=params
        )

    def rmvdiskaccess(self, vdisk, iogrp):
        method = ('block/vdiskaccess/%s?iogrp=%s' % (vdisk, iogrp))
        request_type = 'delete'
        self._run_rest(
            method=method, request_type=request_type
        )

    def clearmcscache(self):
        method = 'security/cleanscsicache'
        request_type = 'post'
        self._run_rest(
            method=method, request_type=request_type
        )

    def migratevdisk(self, vdisk, dest_pool, copy_id='0'):
        method = 'dataprotection/migratevdisk'
        request_type = 'put'
        params = {
                'mdiskgrp': dest_pool,
                'copy': int(copy_id),
                'vdisk': vdisk
                }
        self._run_rest(
            method=method, request_type=request_type, params=params
        )
