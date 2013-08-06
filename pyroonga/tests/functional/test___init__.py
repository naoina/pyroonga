# -*- coding: utf-8 -*-

import pytest

import pyroonga


def test_connect_with_default():
    grn = pyroonga.connect()
    assert isinstance(grn, pyroonga.Groonga)
    assert grn.host == '0.0.0.0'
    assert grn.port == 10041
    assert grn.connected is True


def test_connect():
    grn = pyroonga.connect('localhost', 10041)
    assert isinstance(grn, pyroonga.Groonga)
    assert grn.host == 'localhost'
    assert grn.port == 10041
    assert grn.connected is True


@pytest.mark.parametrize(('host', 'port'), (
    ('dummy', 10041),
    ('localhost', 10040),
    ('0.0.0.0', 10042),
))
def test_connect_with_incorrect_params(host, port):
    with pytest.raises(pyroonga.GroongaError):
        pyroonga.connect(host, port)
