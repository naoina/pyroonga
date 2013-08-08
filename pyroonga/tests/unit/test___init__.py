# -*- coding: utf-8 -*-

import random

import pyroonga
from pyroonga.tests import utils, mock


def test_connect_with_default():
    with mock.patch('pyroonga.groonga.Context') as m:
        m.return_value.connect.return_value = 0
        grn = pyroonga.connect()
        assert isinstance(grn, pyroonga.Groonga)
        assert grn.host == '0.0.0.0'
        assert grn.port == 10041
        assert grn.connected is True


def test_connect():
    host = utils.random_string()
    port = random.randint(1025, 65535)
    with mock.patch('pyroonga.groonga.Context') as m:
        m.return_value.connect.return_value = 0
        grn = pyroonga.connect(host, port)
        assert isinstance(grn, pyroonga.Groonga)
        assert grn.host == host
        assert grn.port == port
        assert grn.connected is True
