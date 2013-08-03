# -*- coding: utf-8 -*-

import pytest

import pyroonga


class TestGroonga(object):
    def test_connect_with_default(self):
        grn = pyroonga.connect()
        assert grn.connected is True
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041

    def test_connect_with_params(self):
        grn = pyroonga.connect(host='localhost', port=10041)
        assert grn.connected is True
        assert grn.host == 'localhost'
        assert grn.port == 10041

    @pytest.mark.parametrize(('host', 'port'), (
        ('unknown', 10041),
        ('localhost', 1),
    ))
    def test_connect_with_invalid_params(self, host, port):
        with pytest.raises(pyroonga.GroongaError):
            pyroonga.connect(host=host, port=port)
