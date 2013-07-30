# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

"""Python interface for groonga fulltext search engine.
"""

__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
]

import logging

from pyroonga.groonga import *
from pyroonga.exceptions import *
from pyroonga.odm.attributes import *
from pyroonga.odm.table import *

logger = logging.getLogger(__name__)


def connect(host=None, port=None):
    """Convenience function of pyroonga.Groonga.connect

    Create the Groonga object and connect to groonga server with default
    hostname and port.

    :param host: String of server hostname. Default is 0.0.0.0 (aka localhost).
    :param port: Integer of server port. Default is 10041.
    :returns: :class:`pyroonga.groonga.Groonga`
    """
    grn = Groonga()
    grn.connect(host, port)
    return grn
