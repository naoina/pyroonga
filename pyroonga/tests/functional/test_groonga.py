# -*- coding: utf-8 -*-

import pytest

import _groonga

from pyroonga.exceptions import GroongaError, error_messages
from pyroonga.groonga import Groonga


class TestGroongaWithNotConnected(object):
    def test___init__(self):
        # test with default encoding
        grn = Groonga()
        assert grn.encoding == 'utf-8'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None

    @pytest.mark.parametrize('encoding', (
        'utf-8',
        'euc-jp',
        'sjis',
        'latin1',
        'koi8-r',
    ))
    def test___init__with_encoding(self, encoding):
        grn = Groonga(encoding=encoding)
        assert grn.encoding == encoding
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None

    def test_connect_not_running_server(self):
        grn = Groonga()
        with pytest.raises(GroongaError):
            grn.connect(host='dummy', port=1)


class TestGroonga(object):
    def test_connect(self):
        # test the connect
        grn = Groonga()
        grn.connect(host='localhost', port=10041)
        assert grn.connected is True
        assert grn.host == 'localhost'
        assert grn.port == 10041

    def test_reconnect_with_not_connected(self):
        grn = Groonga()
        ctx = grn._ctx
        with pytest.raises(GroongaError):
            grn.reconnect()
        assert grn._ctx is ctx
        assert grn.connected is False

    def test_reconnect_with_connected(self):
        grn = Groonga()
        ctx = grn._ctx
        grn.host = 'localhost'
        grn.port = 10041
        assert grn._ctx is ctx
        assert grn.connected is False
        grn.reconnect()
        assert grn._ctx is not ctx
        assert grn.connected is True

    def test_query_with_not_connected(self):
        grn = Groonga()
        with pytest.raises(GroongaError):
            grn.query('a')

    def test_query_with_invalid_command(self):
        grn = Groonga()
        grn.connect('localhost', 10041)
        with pytest.raises(GroongaError):
            grn.query('a')

    def test_query(self):
        grn = Groonga()
        grn.connect('localhost', 10041)
        result = grn.query('cache_limit')
        assert result == '100'

    def test_query_with_after_invalid_command(self):
        grn = Groonga()
        grn.connect('localhost', 10041)
        with pytest.raises(GroongaError):
            grn.query('unknown command')
        result = grn.query('cache_limit')
        assert result == '100'

    def test__raise_if_notsuccess_with_success(self):
        grn = Groonga()
        try:
            grn._raise_if_notsuccess(_groonga.SUCCESS, "", "")
        except GroongaError:
            assert False, "GroongaError has been raised"

    @pytest.mark.parametrize('rc', (
        rc for rc in error_messages if rc != _groonga.SUCCESS
    ))
    def test__raise_if_notsuccess_with_not_success(self, rc):
        grn = Groonga()
        with pytest.raises(GroongaError):
            grn._raise_if_notsuccess(rc, "", "")
