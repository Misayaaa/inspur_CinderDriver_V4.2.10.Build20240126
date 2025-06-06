"""
Common utils for inspur driver
"""

import re
import base64
import rsa
import functools
import six
import sys
import inspect
import logging as pylogging
from rsa import common

try:
    # the configuration is work since Kilo
    from oslo_log import log as logging
    from oslo_utils import timeutils
except ImportError:
    # the configuration is work before Juno(incuding Juno)
    from cinder.openstack.common import log as logging
    from oslo.config import timeutils

LOG = logging.getLogger(__name__)


# Ignore the failed rest response, go through as success.
FAIL_HANDLER_IGNORE = 'ignore'
# Raise exception for the failed rest response.
FAIL_HANDLER_RAISE = 'raise'
# the failed message pattern of rest access, example "Error(964): []".
REST_RESPONSE_ERROR_PATTERN = re.compile('Error\((.*)\):', re.I)

# QoS limit: 1)BANDWIDTH:100KB/s~10GB/s; 2)IOPS:10~1000000
MIN_BANDWIDTH = 1024 * 100
MAX_BANDWIDTH = 10 * 1024 * 1024 * 1024
MIN_IOPS = 10
MAX_IOPS = 1000000

def inspur_ip_address(ip):
    class ipaddr(object):
        def __init__(self, ip, version):
            self.version = version
            self.ip = ip
    if re.match(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
                "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$", ip):
        return ipaddr(ip, 4)
    else:
        return ipaddr(ip, 6)

def inspur_trace(f):
    func_name = f.__name__

    @functools.wraps(f)
    def trace_logging_wrapper(*args, **kwargs):
        if len(args) > 0:
            maybe_self = args[0]
        else:
            maybe_self = kwargs.get('self', None)

        logger = LOG

        # NOTE(ameade): Don't bother going any further if DEBUG log level
        # is not enabled for the logger.
        if not logger.isEnabledFor(pylogging.DEBUG) and \
           func_name in LogFilter.LOGGING_ONLY_WHEN_DEBUG:
            return f(*args, **kwargs)

        all_args = inspect.getcallargs(f, *args, **kwargs)
        logger.info('==> %(func)s execute begin: call %(all_args)r',
                    {'func': func_name, 'all_args': all_args})

        # start_time = time.time() * 1000
        start = timeutils.utcnow()
        try:
            result = f(*args, **kwargs)
        except Exception as exc:
            end = timeutils.utcnow()
            total_time = (end - start).total_seconds()
            logger.error('<== %(func)s execute error: '
                         'start at <%(start)s>, end at <%(end)s>,'
                         ' consume <%(time).4f>s, -- '
                         'exception %(exc)r',
                         {'func': func_name,
                          'start': start.isoformat(),
                          'end': end.isoformat(),
                          'time': total_time,
                          'exc': exc})
            raise
        end = timeutils.utcnow()
        total_time = (end - start).total_seconds()

        logger.info('<== %(func)s execute successfully: '
                    'start at <%(start)s>, end at <%(end)s>,'
                    ' consume <%(time).4f>s, -- '
                    'return %(result)r.',
                    {'func': func_name,
                     'start': start.isoformat(),
                     'end': end.isoformat(),
                     'time': total_time,
                     'result': result})
        return result
    return trace_logging_wrapper

def convert_str(text):
    """Convert to native string.

    Convert bytes and Unicode strings to native strings:

    * convert to bytes on Python 2:
      encode Unicode using encodeutils.safe_encode()
    * convert to Unicode on Python 3: decode bytes from UTF-8
    """
    if six.PY2:
        return to_utf8(text)
    else:
        if isinstance(text, bytes):
            return text.decode('utf-8')
        else:
            return text

def to_utf8(text):
    """Encode Unicode to UTF-8,return bytes unchanged.

    Raise TypeError if text is not a bytes string or a Unicode string.

    .. versionadded::3.5
    """
    if isinstance(text, six.text_type):
        return text
    elif isinstance(text, six.text_type):
        return text.encode('utf-8')
    else:
        raise TypeError("bytes or Unicode expected, got %s"
                        % type(text).__name__)

utils_trace = inspur_trace


class RsaUtil(object):
    """RSA Algorithm for password"""
    def __init__(self, pri_key_path):
        if pri_key_path:
            with open(pri_key_path) as pri_file:
                self.private_key = rsa.PrivateKey.load_pkcs1(pri_file.read())

    def get_max_length(self, rsa_key, encrypt=True):
        blocksize = common.byte_size(rsa_key.n)
        reserve_size = 11
        if not encrypt:
            reserve_size = 0
        maxlength = blocksize - reserve_size
        return maxlength

    def decrypt_by_private_key(self, message):
        """decrypt by private key."""
        decrypt_result = b""
        max_length = self.get_max_length(self.private_key, False)
        decrypt_message = base64.b64decode(message)
        while decrypt_message:
            input = decrypt_message[:max_length]
            decrypt_message = decrypt_message[max_length:]
            out = rsa.decrypt(input, self.private_key)
            decrypt_result += out
            if six.PY3:
                return str(decrypt_result, encoding='utf8')
        return decrypt_result


class LogFilter(object):
    LOGGING_ONLY_WHEN_DEBUG = []

    @classmethod
    def register_function(cls):
        def deco(func):
            if func.__name__ not in cls.LOGGING_ONLY_WHEN_DEBUG:
                cls.LOGGING_ONLY_WHEN_DEBUG.append(func.__name__)
            return func
        return deco


