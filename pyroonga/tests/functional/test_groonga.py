# -*- coding: utf-8 -*-

import pytest

import _groonga

from pyroonga.exceptions import GroongaError
from pyroonga.groonga import Groonga
from pyroonga.tests.functional import (unittest, GroongaTestBase)


class TestGroongaWithNotConnected(unittest.TestCase):
    def test___init__(self):
        # test with default encoding
        grn = Groonga()
        assert grn.encoding == 'utf-8'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None

        # test with all encodings
        grn = Groonga(encoding='utf-8')
        assert grn.encoding == 'utf-8'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None
        grn = Groonga(encoding='euc-jp')
        assert grn.encoding == 'euc-jp'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None
        grn = Groonga(encoding='sjis')
        assert grn.encoding == 'sjis'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None
        grn = Groonga(encoding='latin1')
        assert grn.encoding == 'latin1'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None
        grn = Groonga(encoding='koi8-r')
        assert grn.encoding == 'koi8-r'
        assert grn.connected is False
        assert grn.host is None
        assert grn.port is None

    def test_connect_not_running_server(self):
        grn = Groonga()
        with pytest.raises(GroongaError):
            grn.connect(host='dummy', port=1)


class TestGroonga(GroongaTestBase):
    def test_connect(self):
        # test the connect
        grn = Groonga()
        grn.connect(host='localhost', port=10041)
        assert grn.connected is True
        assert grn.host == 'localhost'
        assert grn.port == 10041

    def test_reconnect(self):
        # test the reconnect with not connected
        grn = Groonga()
        ctx = grn._ctx
        with pytest.raises(GroongaError):
            grn.reconnect()
        assert grn._ctx is ctx
        assert grn.connected is False

        # test the reconnect
        grn = Groonga()
        ctx = grn._ctx
        grn.host = 'localhost'
        grn.port = 10041
        grn.reconnect()
        assert grn._ctx is not ctx
        assert grn.connected is True

    def test_query(self):
        # test with not connected
        grn = Groonga()
        with pytest.raises(GroongaError):
            grn.query('a')

        # test with invalid command
        grn = Groonga()
        grn.connect('localhost', 10041)
        with pytest.raises(GroongaError):
            grn.query('a')

        # test the query
        grn = Groonga()
        grn.connect('localhost', 10041)
        result = grn.query('cache_limit')
        assert result == '100'

        # test the query with after the query of invalid command
        grn = Groonga()
        grn.connect('localhost', 10041)
        with pytest.raises(GroongaError):
            grn.query('unknown command')
        result = grn.query('cache_limit')
        assert result == '100'

    def test__raise_if_notsuccess(self):
        grn = Groonga()
        try:
            grn._raise_if_notsuccess(_groonga.SUCCESS, "", "")
        except GroongaError:
            assert False, "GroongaError has been raised"
        from pyroonga.exceptions import error_messages
        for rc in [rc for rc in error_messages if rc != _groonga.SUCCESS]:
            with pytest.raises(GroongaError):
                grn._raise_if_notsuccess(rc, "", "")


def main():
    unittest.main()

if __name__ == '__main__':
    main()
