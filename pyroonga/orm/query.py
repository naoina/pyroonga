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
]

import itertools
import json
import logging

from pyroonga import utils

logger = logging.getLogger(__name__)


class QueryError(Exception):
    def __init__(self, msg):
        self.msg

    def __str__(self):
        return self.msg


class Query(object):
    """Query representation base class"""

    def __init__(self, tbl):
        """Construct of query

        :param tbl: Table class. It will be created using
            :func:`pyroonga.orm.table.tablebase`\ .
        """
        self._table = tbl


class GroongaResultBase(object):
    """Base class of query result"""

    def __init__(self, cls, results, maxlen=None):
        """Construct of GroongaResultBase

        :param cls: Class for mappings.
        :param results: query results.
        :param maxlen: maximum length of mapping results. Default is all.
        """
        cols = [col[0] for col in results[1]]
        colrange = range(len(cols))
        result = []
        for v in results[2:maxlen]:
            mapped = dict(zip(cols, [v[i] for i in colrange]))
            result.append(cls(**mapped))
        self._all_len = results[0][0]
        self._result = result

    @property
    def all_len(self):
        """All length of query results

        Note that the number of elements is not necessarily equal.
        """
        return self._all_len

    def __len__(self):
        return len(self._result)

    def __iter__(self):
        return iter(self._result)

    def __getitem__(self, key):
        return self._result[key]

    def __reversed__(self):
        return reversed(self._result)


class GroongaResult(GroongaResultBase):
    """Result class for 'select' query"""

    def __init__(self, table, resultstr, maxlen=None):
        """Construct of GroongaResult

        :param table: Table class for mappings.
        :param resultstr: result string of 'select' query.
        :param maxlen: maximum length of mapping results. Default is all.
        """
        objs = json.loads(resultstr)
        super(GroongaResult, self).__init__(table, objs[0], maxlen)
        self._drilldown = self._drilldown_mapping(objs[1:])
        self._table = table

    def _drilldown_mapping(self, results):
        drilldown = [GroongaDrilldownResult(Drilldown, v) for v in results]
        return drilldown

    @property
    def drilldown(self):
        """List of instance of `GroongaDrilldownResult`"""
        return self._drilldown


class GroongaDrilldownResult(GroongaResultBase):
    """Result class for drilldown"""


class Drilldown(object):
    """Drilldown representation class"""

    def __init__(self, _key=None, _nsubrecs=None):
        self._key = _key
        self._nsubrecs = _nsubrecs


class SelectQueryBase(Query):
    """'select' query representation base class"""

    __options__ = {'limit':  '',
                   'offset': '',
                   'sortby': '',
                   'output_columns': ''}

    def __init__(self, tbl, *args, **kwargs):
        """Construct of 'select' query

        :param tbl: Table class. see also :class:`Query`\ .
        :param args: :class:`ExpressionTree`\ .
        :param kwargs: search columns and search texts.
        """
        super(SelectQueryBase, self).__init__(tbl)
        self._expr = args
        self._target = kwargs
        self._limit = None
        self._offset = None
        self._sortby = []
        self._output_columns = []
        self._cache = True
        self._match_escalation_threshold = None

    def all(self):
        """Obtain the all result from this query instance

        :returns: result of query as a Python's objects. (dict, list, etc...)
        """
        q = str(self)
        result = self._table.grn.query(q)
        return GroongaResult(self._table, result)

    def limit(self, lim):
        """Limit for number of result of query

        :param lim: max number of results.
        :returns: self. for method chain.
        """
        self._limit = int(lim)
        return self

    def offset(self, off):
        """Offset for start position of result of query

        :param off: offset. Base is 0.
        :returns: self. for method chain.
        """
        self._offset = int(off)
        return self

    def sortby(self, *args):
        """Set the sort order for result of query

        :param args: :class:`pyroonga.orm.table.Column` of sort keys.
        :returns: self. for method chain.
        """
        self._sortby = args
        return self

    def output_columns(self, *args):
        """Select the output columns for result of query

        :param args: :class:`pyroonga.orm.table.Column`
        :returns: self. for method chain.
        """
        self._output_columns = args
        return self

    def cache(self, iscache):
        """Set the query cache

        :param iscache: no query cache if False. otherwise **yes**\ .
        :returns: self. for method chain.
        """
        self._cache = bool(iscache)
        return self

    def match_escalation_threshold(self, threshold):
        """Set the match escalation threshold

        :param threshold: threshold of match escalation
        :returns: self. for method chain.
        """
        self._match_escalation_threshold = int(threshold)
        return self

    def _makelimit(self):
        if self._limit:
            return '%s %d' % (self.__options__['limit'], self._limit)
        else:
            return ''

    def _makeoffset(self):
        if self._offset:
            return '%s %d' % (self.__options__['offset'], self._offset)
        else:
            return ''

    def _makesortby(self):
        if self._sortby:
            keys = ['-' * key._desc + key.name for key in self._sortby]
            return '%s %s' % (self.__options__['sortby'], ','.join(keys))
        else:
            return ''

    def _makeoutput_columns(self):
        if self._output_columns:
            cols = [col.name for col in self._output_columns]
            return '%s %s' % (self.__options__['output_columns'],
                              ','.join(cols))
        else:
            return ''

    def _makecache(self):
        return '' if self._cache else '--cache no'

    def _makematch_escalation_threshold(self):
        if self._match_escalation_threshold is None:
            return ''
        else:
            return ('--match_escalation_threshold %d' %
                    self._match_escalation_threshold)

    def _makeparams(self):
        return ''

    def _condition(self):
        return '%(cache)s %(limit)s %(offset)s %(sortby)s %(output_columns)s ' \
               '%(match_escalation_threshold)s %(params)s' % \
               dict(cache=self._makecache(),
                    limit=self._makelimit(),
                    offset=self._makeoffset(),
                    sortby=self._makesortby(),
                    output_columns=self._makeoutput_columns(),
                    match_escalation_threshold=self._makematch_escalation_threshold(),
                    params=self._makeparams())

    def __str__(self):
        return 'select --table "%(table)s" %(condition)s' % dict(
                table=self._table.__tablename__,
                condition=self._condition())


class SelectQuery(SelectQueryBase):
    """Query representation class for 'select' query"""

    __options__ = {'limit':  '--limit',
                   'offset': '--offset',
                   'sortby': '--sortby',
                   'output_columns': '--output_columns'}

    def drilldown(self, *columns):
        """Switch to the drilldown query

        :param columns: target columns for drilldown.
        :returns: :class:`DrillDownQuery`\ .
        """
        return DrillDownQuery(self, *columns)

    def _makeparams(self):
        params = [utils.escape('%s:@"%s"' % target) for target in
                  self._target.items()]
        param = Expression.OR.join(params)
        exprs = [utils.escape(self._makeexpr(expr)) for expr in self._expr]
        expr = Expression.OR.join(exprs)
        result = param and '(%s)' % param
        if result and expr:
            result += Expression.AND
        result += expr
        return '--query "%s"' % result if result else ''

    def _makeexpr(self, expr):
        if isinstance(expr, ExpressionTree):
            return '(%s%s%s)' % (self._makeexpr(expr.left), expr.expr,
                    self._makeexpr(expr.right))
        elif isinstance(expr, Value):
            return '"%s"' % expr
        else:
            return str(expr)


class DrillDownQuery(SelectQueryBase):
    """'select' query with drilldown representation class

    Instantiate from :meth:`SelectQuery.drilldown`\ .
    """

    __options__ = {'limit':  '--drilldown_limit',
                   'offset': '--drilldown_offset',
                   'sortby': '--drilldown_sortby',
                   'output_columns': '--drilldown_output_columns'}

    def __init__(self, parent, *args):
        """Construct of drilldown query

        :param parent: parent :class:`SelectQuery`\ .
        :param args: target columns for drilldown. Type is
            :class:`pyroonga.orm.table.Column`\ .
        """
        if not args:
            raise ValueError("args is must be one or more columns")
        super(DrillDownQuery, self).__init__(parent._table)
        self.parent = parent
        self.columns = args

    def _makeparams(self):
        cols = [col.name for col in self.columns]
        return ('--drilldown %s' % ','.join(cols)) if cols else ''

    def __str__(self):
        return str(self.parent) + (' %s' % self._condition())


class Value(object):
    slots = ['value']

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)


class Expression(object):
    """Expression constants"""

    EQUAL         = ':'
    GREATER_EQUAL = ':>='
    GREATER_THAN  = ':>'
    LESS_EQUAL    = ':<='
    LESS_THAN     = ':<'
    NOT_EQUAL     = ':!'
    OR  = ' OR '
    AND = ' + '
    NOT = ' - '


class ExpressionTree(object):
    """Query conditional expression tree class"""

    slots = ['expr', 'left', 'right']

    def __init__(self, expr, left=None, right=None):
        self.expr = expr
        self.left = left
        self.right = right

    def __eq__(self, other):
        return ExpressionTree(Expression.EQUAL, self, other)

    def __ge__(self, other):
        return ExpressionTree(Expression.GREATER_EQUAL, self, other)

    def __gt__(self, other):
        return ExpressionTree(Expression.GREATER_THAN, self, other)

    def __le__(self, other):
        return ExpressionTree(Expression.LESS_EQUAL, self, other)

    def __lt__(self, other):
        return ExpressionTree(Expression.LESS_THAN, self, other)

    def __ne__(self, other):
        return ExpressionTree(Expression.NOT_EQUAL, self, other)

    def __and__(self, other):
        return ExpressionTree(Expression.AND, self, other)

    def __or__(self, other):
        return ExpressionTree(Expression.OR, self, other)

    def __sub__(self, other):
        return ExpressionTree(Expression.NOT, self, other)


class LoadQuery(Query):
    """'load' query representation class"""

    def __init__(self, tbl, data):
        """Construct of LoadQuery

        :param tbl: Table class. see also :class:`Query`\ .
        :param data: iterable object of instance of Table.
        """
        super(LoadQuery, self).__init__(tbl)
        self._data = iter(data)

    def load(self, data):
        """Prepare the load data

        :param data: iterable or Table object
        :returns: self. for method chain.
        """
        try:
            data = iter(data)
        except TypeError:
            data = [data]
        self._data = itertools.chain(self._data, data)
        return self

    def commit(self):
        """Load data to groonga actually

        :returns: number of loaded data
        """
        if self._data is None:
            raise RuntimeError('query is already commited or rollbacked')
        q = str(self)
        result = int(self._table.grn.query(q))
        self.rollback()
        return result

    def rollback(self):
        self._data = None

    def _makejson(self):
        return utils.escape(json.dumps([v.asdict() for v in self._data]))

    def __str__(self):
        return 'load --table %(table)s --input_type json --values ' \
               '"%(data)s"' % dict(table=self._table.__name__,
                                   data=self._makejson())
