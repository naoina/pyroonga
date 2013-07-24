# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
    'Groonga',
]

import json
import logging

import _groonga
from pyroonga.exceptions import GroongaError

logger = logging.getLogger(__name__)

DEFAULT_ENCODING = _groonga.ENC_UTF8

encodings = {
    'utf-8': _groonga.ENC_UTF8,
    'euc-jp': _groonga.ENC_EUC_JP,
    'sjis': _groonga.ENC_SJIS,
    'latin1': _groonga.ENC_LATIN1,
    'koi8-r': _groonga.ENC_KOI8R}


class Context(_groonga.Context):
    def __init__(self, encoding):
        """Construct a Context of groonga

        :param encoding: Encoding of groonga. Supported value is 'utf-8',
            'euc-jp', 'sjis', 'latin1', and 'koi8-r'.
        """
        super(Context, self).__init__(flags=0)
        encodename = encoding.replace('_', '-')
        enc = encodings.get(encodename, DEFAULT_ENCODING)
        self.set_encoding(enc)


class Groonga(object):
    def __init__(self, encoding='utf-8'):
        """Construct a Groonga.

        :param encoding: Encoding of groonga. Supported value is 'utf-8',
            'euc-jp', 'sjis', 'latin1' and 'koi8-r'. Default is 'utf-8'.
        """
        self._ctx = Context(encoding)
        self.encoding = encoding
        self.connected = False
        self.host = self.port = None

    def connect(self, host=None, port=None):
        """Connect to the groonga server

        :param host: String of server hostname.
        :param port: Integer of server port number.
        """
        self.host = host or '0.0.0.0'
        self.port = port or 10041
        rc = self._ctx.connect(self.host, self.port, flags=0)
        self._raise_if_notsuccess(rc, "", "")
        self.connected = True

    def reconnect(self):
        """Reconnect to the groonga server
        """
        if self.host is None or self.port is None:
            raise GroongaError(_groonga.SOCKET_IS_NOT_CONNECTED)
        del self._ctx
        self._ctx = Context(self.encoding)
        self.connect(self.host, self.port)
        self.connected = True

    def query(self, qstr):
        """Send and receive the query string to the groonga server

        :param qstr: Query string.
        :returns: Result string.
        """
        if not self.connected:
            raise GroongaError(_groonga.SOCKET_IS_NOT_CONNECTED)
        logger.debug(qstr)
        self._ctx.send(qstr, flags=0)
        rc, result, flags = self._ctx.recv()
        try:
            self._raise_if_notsuccess(rc, result, qstr)
        except GroongaError:
            self.reconnect()
            raise
        return result

    def _raise_if_notsuccess(self, rc, msg, query):
        if rc != _groonga.SUCCESS:
            try:
                msg = json.loads(msg)[0][3]
            except (IndexError, ValueError, TypeError):
                pass
            self.connected = False
            raise GroongaError(rc, msg, query)
