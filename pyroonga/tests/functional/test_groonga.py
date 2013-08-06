# -*- coding: utf-8 -*-

import pytest

from pyroonga.exceptions import GroongaError
from pyroonga.groonga import Groonga


class TestGroonga(object):
    @pytest.mark.parametrize(('host', 'port'), (
        ('dummy', 10041),
        ('localhost', 10040),
        ('0.0.0.0', 10042),
    ))
    def test_connect_with_incorrect_params(self, host, port):
        grn = Groonga()
        with pytest.raises(GroongaError):
            grn.connect(host=host, port=port)

    def test_connect_with_default(self):
        grn = Groonga()
        assert grn.connected is False
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        grn.connect()
        assert grn.connected is True
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041

    def test_connect_with_params(self):
        grn = Groonga()
        assert grn.connected is False
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        grn.connect(host='localhost', port=10041)
        assert grn.connected is True
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041

    def test_reconnect(self):
        grn = Groonga()
        ctx = grn._ctx
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
        grn.connect()
        with pytest.raises(GroongaError):
            grn.query('a')

    def test_query(self):
        grn = Groonga()
        grn.connect()
        result = grn.query('cache_limit')
        assert result == '100'

    def test_query_with_after_invalid_command(self):
        grn = Groonga()
        grn.connect()
        with pytest.raises(GroongaError):
            grn.query('unknown command')
        result = grn.query('cache_limit')
        assert result == '100'
