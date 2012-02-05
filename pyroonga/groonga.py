# -*- coding: utf-8 -*-

# Copyright (c) 2012 Naoya INADA <naoina@kuune.org>
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.


__author__ = "Naoya INADA <naoina@kuune.org>"

__all__ = [
    'Groonga',
]

import logging

logger = logging.getLogger(__name__)

import _groonga
from pyroonga.exceptions import GroongaError

DEFAULT_ENCODING = _groonga.ENC_UTF8

encodings = {
    'utf-8': _groonga.ENC_UTF8,
    'euc-jp': _groonga.ENC_EUC_JP,
    'sjis': _groonga.ENC_SJIS,
    'latin1': _groonga.ENC_LATIN1,
    'koi8-r': _groonga.ENC_KOI8R,
    }


class Groonga(object):
    def __init__(self, encoding='utf-8'):
        ctx = _groonga.Context(flags=0)
        encodename = encoding.replace('_', '-')
        enc = encodings.get(encodename, DEFAULT_ENCODING)
        ctx.set_encoding(enc)
        self.ctx = ctx
        self.connected = False

    def connect(self, host, port):
        rc = self.ctx.connect(host, port, flags=0)
        if rc != _groonga.SUCCESS:
            raise GroongaError(rc)
        self.connected = True
        self.host = host
        self.port = port

    def query(self, qstr):
        self.ctx.send(qstr, flags=0)
        result, flags = self.ctx.recv()
        return result


def main():
    pass

if __name__ == '__main__':
    main()
