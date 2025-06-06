# -*- coding: UTF-8 -*-
#!/usr/bin/env python
import sys
import os
import base64
import rsa
import six
from rsa import common


class RsaUtil(object):

    def __init__(self, pub_key_path):

        if pub_key_path:
            with open(pub_key_path) as pub_file:
                self.public_key = \
                    rsa.PublicKey.load_pkcs1_openssl_pem(pub_file.read())

    def get_max_length(self, rsa_key, encrypt=True):
        blocksize = common.byte_size(rsa_key.n)
        reserve_size = 11
        if not encrypt:
            reserve_size = 0
        maxlength = blocksize - reserve_size
        return maxlength

    def encrypt_by_public_key(self, message):
        encrypt_result = b''
        max_length = self.get_max_length(self.public_key)
        while message:
            input = message[:max_length]
            message = message[max_length:]
            if six.PY3:
                out = rsa.encrypt(bytes(input,encoding='utf8'), self.public_key)
            else:
                out = rsa.encrypt(input, self.public_key)
            encrypt_result += out
        encrypt_result = base64.b64encode(encrypt_result)
        return encrypt_result


if __name__ == '__main__':
    args_count = len(sys.argv)
    if args_count != 3:
        print("usage: python cipher.py <pub_key_path> <message>")
        sys.exit(1)
    pub_key_path = sys.argv[1]
    message = sys.argv[2]
    if not os.path.exists(pub_key_path):
        print("public_key is not exist,please check")
        sys.exit(2)

    rsaUtil = RsaUtil(pub_key_path)
    encrypy_result = rsaUtil.encrypt_by_public_key(message)
    if six.PY3:
        print(str(encrypy_result,encoding='utf8'))
    else:
        print(encrypy_result)
