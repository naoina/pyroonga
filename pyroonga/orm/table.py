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
    'Column', 'prop_attr', 'tablebase',
]

import logging

from pyroonga.orm.attributes import *

logger = logging.getLogger(__name__)


class Column(object):
    """Column representation class

    e.g. ::

        class ExampleTable(Table):
            name = Column()
            age  = Column(flags=COLUMN_SCALAR, type=UInt8)

    The above example is equivalent to the table that will be create in the
    following query::

        table_create --name ExampleTable --flags TABLE_HASH_KEY --key_type ShortText
        column_create --table ExampleTable --name name --flags COLUMN_SCALAR --type ShortText
        column_create --table ExampleTable --name age --flags COLUMN_SCALAR --type UInt8
    """

    def __init__(self, flags=COLUMN_SCALAR, type=ShortText):
        """Construct of table column

        :param flags: const of :class:`ColumnFlags`\ . Default is
            :const:`COLUMN_SCALAR`\ .
        :param type: const of :class:`DataType`\ . Default is
            :const:`ShortText`\ .
        """
        if not isinstance(flags, ColumnFlags):
            raise TypeError('"flags" is must be instance of ColumnFlags')
        if not isinstance(type, DataType):
            raise TypeError('"type" is must be instance of DataType')
        self.flags = flags
        self.type = type
        self.tablename = self.name = self.value = None

    def __str__(self):
        if not (self.tablename and self.name):
            raise TypeError('column instance is not initialized')
        return ('column_create --table %s --name %s --flags %s --type %s' %
                (self.tablename, self.name, self.flags, self.type))


class TableMeta(type):
    def __init__(cls, name, bases, dict_):
        if '_tables' not in dict_:
            cls._tables.append(cls)
            cls.columns = []
            for k, v in cls.__dict__.items():
                if isinstance(v, Column):
                    cls._setcolumn(k, v)
        return type.__init__(cls, name, bases, dict_)

    def _setcolumn(cls, name, col):
        col.name = name
        col.tablename = cls.__tablename__
        cls.columns.append(col)

    def __str__(cls):
        return ('table_create --name %s --flags %s --key_type %s' %
                (cls.__tablename__, cls.__tableflags__, cls.__key_type__))


class prop_attr(property):
    """Property decorator for class method 

    Like @property decorator, but it can applicable to the class method::

        class Example(object):
            @prop_attr
            def __tablename__(cls):
                return cls.__name__.lower()
    """

    def __get__(self, instance, owner):
        return self.fget(owner)


class TableBase(object):
    """Table representation base class

    The table name is same as class name if not specified the
    :attr:`__tablename__`\ . Otherwise, table name uses the
    :attr:`__tablename__`\ .

    Default flags is :const:`TABLE_HASH_KEY`
    And default key_type is :const:`ShortText`\ .

    e.g. ::

        Table = tablebase()

        class ExampleTable(Table):
            pass

    The above example is equivalent to the table that will be create in the
    following query::

        table_create --name ExampleTable --flags TABLE_HASH_KEY --key_type ShortText
    """

    __tableflags__ = TABLE_HASH_KEY
    __key_type__   = ShortText

    @prop_attr
    def __tablename__(cls):
        return cls.__name__

    @classmethod
    def create_all(cls, grn):
        """Create the all defined tables and columns

        :param grn: instance of :class:`pyroonga.groonga.Groonga`\ .
        """
        if not grn.connected:
            grn.connect()
        queries = []
        for tbl in cls._tables:
            queries.append(str(tbl))
            queries.extend([str(col) for col in tbl.columns])
        for query in queries:
            logger.debug(query)
            grn.query(query)


def tablebase(name='Table', cls=TableBase):
    return TableMeta(name, (cls,), {'_tables': []})
