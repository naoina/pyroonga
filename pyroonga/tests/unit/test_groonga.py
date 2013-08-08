# -*- coding: utf-8 -*-

import random

import pytest

import _groonga

from pyroonga.exceptions import GroongaError, error_messages
from pyroonga.groonga import Groonga

from pyroonga.tests import utils, mock


class TestGroonga(object):
    @pytest.mark.parametrize('encoding', (
        'utf-8',
        'euc-jp',
        'sjis',
        'latin1',
        'koi8-r',
    ))
    def test___init__with_encoding(self, encoding):
        grn = Groonga(encoding=encoding)
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        assert grn.encoding == encoding
        assert grn.connected is False

    def test___init___with_default_params(self):
        grn = Groonga()
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        assert grn.encoding == 'utf-8'
        assert grn.connected is False

    def test___init___with_params(self):
        host = utils.random_string()
        port = random.randint(0, 65535)
        encoding = random.choice(['euc-jp', 'sjis', 'latin1', 'koi8-r'])
        grn = Groonga(host=host, port=port, encoding=encoding)
        assert grn.host == host
        assert grn.port == port
        assert grn.encoding == encoding
        assert grn.connected is False

    def test_connect_with_default(self):
        grn = Groonga()
        grn._ctx = mock.MagicMock()
        grn._ctx.connect.return_value = 0
        assert grn.connected is False
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        grn.connect()
        assert grn.connected is True
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        assert grn._ctx.connect.mock_calls == [mock.call('0.0.0.0', 10041,
                                                         flags=0)]

    def test_connect_with_params(self):
        host = utils.random_string()
        port = random.randint(1025, 65535)
        grn = Groonga()
        grn._ctx = mock.MagicMock()
        grn._ctx.connect.return_value = 0
        assert grn.connected is False
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        grn.connect(host=host, port=port)
        assert grn.connected is True
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        assert grn._ctx.connect.mock_calls == [mock.call(host, port, flags=0)]

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
