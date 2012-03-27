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
    'Column', 'prop_attr', 'tablebase', 'SuggestTable', 'event_type',
    'bigram', 'kana', 'item_query', 'pair_query', 'sequence_query',
    'event_query',
]

import logging

from pyroonga.groonga import Groonga
from pyroonga.orm.attributes import (
    TableFlags,
    ColumnFlagsFlag,
    ColumnFlags,
    DataType,
    TokenizerSymbol,
    Tokenizer,
    )
from pyroonga.orm.query import (
    Expression,
    ExpressionTree,
    LoadQuery,
    SuggestQuery,
    SuggestLoadQuery,
    SelectQuery,
    Value,
    )

logger = logging.getLogger(__name__)


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


class TableMeta(type):
    def __init__(cls, name, bases, dict_):
        if '_tables' not in dict_:
            cls._tables.append(cls)
            cls.columns = []
            for k, v in cls.__dict__.items():
                if isinstance(v, Column):
                    cls._setcolumn(k, v)
                    cls.columns.append(v)
            cls._set_pseudocolumns()
        return type.__init__(cls, name, bases, dict_)

    def _set_pseudocolumns(cls):
        for attr, name, typ in (('_id', '_id', DataType.UInt32),
                                ('_nsubrecs', '_nsubrecs', DataType.Int32),
                                ('ALL', '*', DataType.ShortText)):
            col = Column(flags=ColumnFlags.COLUMN_SCALAR, type=typ)
            setattr(cls, attr, col)
            cls._setcolumn(name, col)
        if not cls._has_table_no_key():
            cls._key = Column(flags=ColumnFlags.COLUMN_SCALAR, type=cls.__key_type__)
            cls._setcolumn('_key', cls._key)

    def _has_table_no_key(cls):
        return cls.__tableflags__ & TableFlags.TABLE_NO_KEY

    def _setcolumn(cls, name, col):
        col.name = name
        col.tablename = cls.__tablename__

    def __str__(cls):
        flags = ['--name %s' % cls.__tablename__,
                 '--flags %s' % cls.__tableflags__]
        if not cls._has_table_no_key():
            flags.append('--key_type %s' % cls.__key_type__)
        if isinstance(cls.__default_tokenizer__, TokenizerSymbol):
            flags.append('--default_tokenizer %s' % cls.__default_tokenizer__)
        return 'table_create ' + (' '.join(flags))


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

    __tableflags__ = TableFlags.TABLE_HASH_KEY
    __key_type__   = DataType.ShortText
    __default_tokenizer__ = None
    grn = None

    @prop_attr
    def __tablename__(cls):
        return cls.__name__

    def __init__(self, **kwargs):
        """Construct of TableBase

        :param kwargs: name and value of columns
        """
        for k, v in kwargs.items():
            try:
                object.__getattribute__(self.__class__, k)
            except AttributeError:
                raise AttributeError('key "%s" is not defined in %s' % (k,
                    self.__class__.__name__))
            else:
                setattr(self, TableBase._attrname(k), v)

    @classmethod
    def _attrname(self, base):
        return '%s_' % base

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, TableBase._attrname(name))
        except AttributeError:
            return object.__getattribute__(self, name)

    @classmethod
    def bind(cls, grn):
        """Bind the :class:`pyroonga.groonga.Groonga` object to the this table

        :param grn: :class:`pyroonga.groonga.Groonga` object.
        """
        if not isinstance(grn, Groonga):
            raise TypeError("not %s instance" % Groonga.__name__)
        if not grn.connected:
            grn.connect()
        cls.grn = grn

    @classmethod
    def create_all(cls):
        """Create the all defined tables and columns

        :param grn: instance of :class:`pyroonga.groonga.Groonga`\ .
        """
        if not isinstance(cls.grn, Groonga):
            raise TypeError("%s object is not bind" % Groonga.__name__)
        table_queries  = []
        column_queries = []
        for tbl in cls._tables:
            table_queries.append(str(tbl))
            column_queries.extend(str(col) for col in tbl.columns)
        for queries in (table_queries, column_queries):
            for query in queries:
                logger.debug(query)
                cls.grn.query(query)

    @classmethod
    def select(cls, *args, **kwargs):
        """Select query to the groonga

        e.g.::

            # returns data that contains "cthulhu" in the title
            Table.select(title='cthulhu').all()

            # returns data that "name" is not "nyarlathotep".
            Table.select(Table.name != 'nyarlathotep').all()

        :param args: :class:`pyroonga.orm.query.ExpressionTree`\ . Created by
            comparison of :class:`Column` and any value.
        :param kwargs: search columns and search texts.
        :returns: :class:`pyroonga.orm.query.SelectQuery`\ .
        """
        query = SelectQuery(cls, *args, **kwargs)
        return query

    @classmethod
    def load(cls, data, immediate=True):
        """Load data to the groonga

        :param data: iterable object of instance of Table.
        :param immediate: load data to groonga immediately if True. Otherwise,
            Must call :meth:`pyroonga.orm.query.LoadQuery.commit` explicitly for data
            load.
        :returns: :class:`pyroonga.orm.query.LoadQuery`\ .
        """
        query = cls._load(data)
        return query.commit() if immediate else query

    @classmethod
    def _load(cls, data):
        return LoadQuery(cls, data)

    def asdict(self):
        return dict((k[:-1], v) for k, v in self.__dict__.items()
                    if v and k.endswith('_'))


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

    __tablemeta__ = TableMeta

    def __init__(self, flags=ColumnFlags.COLUMN_SCALAR,
                 type=DataType.ShortText, source=None):
        """Construct of table column

        :param flags: const of :class:`ColumnFlags`\ .
                      Default is :const:`COLUMN_SCALAR`\ .
        :param type: const of :class:`DataType` or instance of
                     :class:`TableMeta` or str.
                     Default is :const:`ShortText`\ .
        :param source: instance of :class:`Column` or str.
                       Default is None
        """
        if not isinstance(flags, ColumnFlagsFlag):
            raise TypeError('"flags" is must be instance of ColumnFlags')
        self.flags = flags
        if isinstance(type, self.__tablemeta__):
            self.type = type.__tablename__
        else:
            self.type = type
        self.source = source.name if isinstance(source, Column) else source
        self.tablename = self.name = self.value = None
        self._desc = False

    def __eq__(self, other):
        return ExpressionTree(Expression.EQUAL, self.name,
                Value(other))

    def __ge__(self, other):
        return ExpressionTree(Expression.GREATER_EQUAL, self.name,
                Value(other))

    def __gt__(self, other):
        return ExpressionTree(Expression.GREATER_THAN, self.name,
                Value(other))

    def __le__(self, other):
        return ExpressionTree(Expression.LESS_EQUAL, self.name,
                Value(other))

    def __lt__(self, other):
        return ExpressionTree(Expression.LESS_THAN, self.name,
                Value(other))

    def __ne__(self, other):
        return ExpressionTree(Expression.NOT_EQUAL, self.name,
                Value(other))

    def __neg__(self):
        self._desc = True
        return self

    def __str__(self):
        if not (self.tablename and self.name):
            raise TypeError('column instance is not initialized')
        query = ['--table %s' % self.tablename,
                 '--name %s' % self.name,
                 '--flags %s' % self.flags,
                 '--type %s' % self.type]
        if self.source:
            query.append('--source %s' % self.source)
        return 'column_create %s' % (' '.join(query))


def tablebase(name='Table', cls=TableBase):
    """Create the base class of Table definition

    :param name: name of Table. Default is 'Table'.
    :param cls: class of base of base class. Default is :class:`TableBase`\ .
    :returns: base class.
    """
    return TableMeta(name, (cls,), {'_tables': []})


# for suggest
class SuggestTableBase(TableBase):
    """Suggest's table representation base class"""

    @classmethod
    def create_all(cls):
        """Create the all defined tables and columns

        :param grn: instance of :class:`pyroonga.groonga.Groonga`\ .
        """
        if not isinstance(cls.grn, Groonga):
            raise TypeError("%s object is not bind" % Groonga.__name__)
        table_queries  = []
        column_queries = []
        for tbl in cls._tables:
            table_queries.append(str(tbl))
            column_queries.extend([str(col) for col in tbl.columns])
        suggest_query = 'register suggest/suggest'
        cls.grn.query(suggest_query)
        for queries in (table_queries, column_queries):
            for query in queries:
                cls.grn.query(query)

    @classmethod
    def suggest(cls, query):
        """Suggest query to the groonga

        e.g.::

            # suggest type is 'complete' by default. Get a 'complete'
            item_query.suggest('en').get('complete')

            # suggest type is 'correct' and 'complete'. Get a 'correct'
            item_query.suggest('en').types(SuggestType.correct |
                                           SuggestType.complete)['correct']

        See also :class:`pyroonga.orm.attributes.SuggestType` and
            :class:`pyroonga.orm.table.item_query`

        :param query: query string for suggest.
        :returns: :class:`pyroonga.orm.query.SuggestQuery`\ .
        """
        query = SuggestQuery(cls, query)
        return query

    @classmethod
    def _load(cls, data):
        return SuggestLoadQuery(cls, data)

SuggestTable = tablebase(name='SuggestTable', cls=SuggestTableBase)


##############################################################################
# NOTE: Do not change class definition order that inherited the SuggestTable.
#       Because, 'suggest' function will not work.
#       This is a related to a creation order of INDEX columns.
##############################################################################
class event_type(SuggestTable):
    __tableflags__ = TableFlags.TABLE_HASH_KEY
    __key_type__   = DataType.ShortText


class bigram(SuggestTable):
    __tableflags__ = TableFlags.TABLE_PAT_KEY | TableFlags.KEY_NORMALIZE
    __key_type__   = DataType.ShortText
    __default_tokenizer__ = Tokenizer.TokenBigram

    item_query_key = Column(flags=(ColumnFlags.COLUMN_INDEX |
                                   ColumnFlags.WITH_POSITION),
                                   type='item_query', source='_key')


class pair_query(SuggestTable):
    __tableflags__ = TableFlags.TABLE_HASH_KEY
    __key_type__   = DataType.UInt64

    pre   = Column(flags=ColumnFlags.COLUMN_SCALAR, type='item_query')
    post  = Column(flags=ColumnFlags.COLUMN_SCALAR, type='item_query')
    freq0 = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)
    freq1 = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)
    freq2 = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)


# TODO: To allow users to configure name, but name prefix must be 'item_'
class item_query(SuggestTable):
    __tableflags__ = TableFlags.TABLE_PAT_KEY | TableFlags.KEY_NORMALIZE
    __key_type__   = DataType.ShortText
    __default_tokenizer__ = Tokenizer.TokenDelimit

    kana  = Column(flags=ColumnFlags.COLUMN_VECTOR, type='kana')
    freq  = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)
    last  = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Time)
    boost = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)
    freq2 = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)
    buzz  = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Int32)
    co    = Column(flags=ColumnFlags.COLUMN_INDEX,  type='pair_query',
                   source='pre')


class kana(SuggestTable):
    __tableflags__ = TableFlags.TABLE_PAT_KEY | TableFlags.KEY_NORMALIZE
    __key_type__   = DataType.ShortText

    item_query_kana = Column(flags=ColumnFlags.COLUMN_INDEX, type='item_query',
                             source='kana')


class sequence_query(SuggestTable):
    __tableflags__ = TableFlags.TABLE_HASH_KEY
    __key_type__   = DataType.ShortText

    events = Column(flags=(ColumnFlags.COLUMN_VECTOR |
                           ColumnFlags.RING_BUFFER), type='event_query')


class event_query(SuggestTable):
    __tableflags__ = TableFlags.TABLE_NO_KEY

    type = Column(flags=ColumnFlags.COLUMN_SCALAR, type='event_type')
    time = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.Time)
    item = Column(flags=ColumnFlags.COLUMN_SCALAR, type='item_query')
    sequence = Column(flags=ColumnFlags.COLUMN_SCALAR, type='sequence_query')
