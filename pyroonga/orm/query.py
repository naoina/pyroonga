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
from pyroonga.orm.attributes import SuggestType

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


class QueryOptionsMixin(object):
    __options__ = {'limit':  '--limit',
                   'offset': '--offset',
                   'sortby': '--sortby',
                   'output_columns': '--output_columns'}

    def __init__(self):
        self._limit = None
        self._offset = None
        self._sortby = []
        self._output_columns = []

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

    def _condition(self):
        return '%(limit)s %(offset)s %(sortby)s %(output_columns)s' % \
               dict(limit=self._makelimit(),
                    offset=self._makeoffset(),
                    sortby=self._makesortby(),
                    output_columns=self._makeoutput_columns())


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
        # TODO: implements by generator
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


class GroongaSelectResult(GroongaResultBase):
    """Result class for 'select' query"""

    def __init__(self, table, resultstr, maxlen=None):
        """Construct of GroongaSelectResult

        :param table: Table class for mappings.
        :param resultstr: result string of 'select' query.
        :param maxlen: maximum length of mapping results. Default is all.
        """
        objs = json.loads(resultstr)
        super(GroongaSelectResult, self).__init__(table, objs[0], maxlen)
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


class GroongaSuggestResults(object):
    """Results of suggestion representation class"""

    __slots__ = ['complete', 'correct', 'suggest']

    def __init__(self, resultstr):
        result = json.loads(resultstr)
        complete = result.get('complete', [])
        correct  = result.get('correct', [])
        suggest  = result.get('suggest', [])
        self.complete = complete and GroongaSuggestResult(Suggest, complete)
        self.correct  = correct  and GroongaSuggestResult(Suggest, correct)
        self.suggest  = suggest  and GroongaSuggestResult(Suggest, suggest)


class GroongaSuggestResult(GroongaResultBase):
    """Result class for suggest"""


class Suggest(object):
    """Suggest representation class"""

    def __init__(self, _key=None, _score=None):
        self._key = _key
        self._score = _score


class Drilldown(object):
    """Drilldown representation class"""

    def __init__(self, _key=None, _nsubrecs=None):
        self._key = _key
        self._nsubrecs = _nsubrecs


class SelectQueryBase(Query, QueryOptionsMixin):
    """'select' query representation base class"""

    def __init__(self, tbl, *args, **kwargs):
        """Construct of 'select' query

        :param tbl: Table class. see also :class:`Query`\ .
        :param args: :class:`ExpressionTree`\ .
        :param kwargs: search columns and search texts.
        """
        Query.__init__(self, tbl)
        QueryOptionsMixin.__init__(self)
        self._expr = args
        self._target = kwargs
        self._cache = True
        self._match_escalation_threshold = None

    def all(self):
        """Obtain the all result from this query instance

        :returns: result of query as a Python's objects. (dict, list, etc...)
        """
        q = str(self)
        result = self._table.grn.query(q)
        return GroongaSelectResult(self._table, result)

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
        return '%(condition)s %(cache)s %(match_escalation_threshold)s ' \
               '%(params)s' % \
               dict(condition=QueryOptionsMixin._condition(self),
                    cache=self._makecache(),
                    match_escalation_threshold=self._makematch_escalation_threshold(),
                    params=self._makeparams())

    def __str__(self):
        return 'select --table "%(table)s" %(condition)s' % dict(
                table=self._table.__tablename__,
                condition=self._condition())


class SelectQuery(SelectQueryBase):
    """Query representation class for 'select' query"""

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


class DrillDownQuery(SelectQueryBase, QueryOptionsMixin):
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
        SelectQueryBase.__init__(self, parent._table)
        QueryOptionsMixin.__init__(self)
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


class SuggestQuery(Query, QueryOptionsMixin):
    """'suggest' query representation class"""

    __options__ = {'limit':  '--limit',
                   'offset': '--offset',
                   'sortby': '--sortby',
                   'output_columns': '--output_columns',
                   'frequency_threshold': '--frequency_threshold',
                   'conditional_probability_threshold':
                       '--conditional_probability_threshold',
                   'prefix_search': '--prefix_search'}

    def __init__(self, tbl, query):
        """Construct of 'suggest' query

        :param tbl: :class:`pyroonga.orm.table.item_query` class.
            see also :class:`Query`\ .
        :param query: query string for suggest.
        """
        Query.__init__(self, tbl)
        QueryOptionsMixin.__init__(self)
        self._query = query
        self._types = SuggestType.complete
        self._frequency_threshold = None
        self._conditional_probability_threshold = None
        self._prefix_search = None
        self._result = None

    def all(self):
        """Get results of suggest

        :returns: :class:`GroongaSuggestResults`
        """
        query = str(self)
        result = self._table.grn.query(query)
        return GroongaSuggestResults(result)

    def get(self, type_):
        """Get a result of suggest by type name

        :param type_: type name of suggest. 'complete', 'correct' or 'suggest'.
        :returns: :class:`GroongaSuggestResult`\ .
        :raises: KeyError
        """
        result = self.all()
        try:
            return getattr(result, type_)
        except AttributeError:
            raise KeyError(type_)

    def __getitem__(self, key):
        """Get results of suggest by type name

        Same as :meth:`SuggestQuery.get`
        """
        return self.get(key)

    def types(self, types):
        """Set the suggestion types

        :param types: see :class:`pyroonga.orm.attributes.SuggestType`
        :returns: self. for method chain.
        """
        self._types = types
        return self

    def frequency_threshold(self, threshold):
        """Set the frequency threshold

        :param threshold: threshold of frequency
        :returns: self. for method chain.
        """
        self._frequency_threshold = int(threshold)
        return self

    def conditional_probability_threshold(self, threshold):
        """Set the conditional probability threshold

        :param threshold: threshold of conditional probability
        :returns: self. for method chain.
        """
        self._conditional_probability_threshold = float(threshold)
        return self

    def prefix_search(self, isprefixsearch):
        """Set the prefix search

        :param threshold: threshold of conditional probability
        :param isprefixsearch: It specifies whether optional prefix search is
            used or not in completion. 'yes' if True, otherwise 'no'.
        :returns: self. for method chain.
        """
        self._prefix_search = bool(isprefixsearch)
        return self

    def _makefrequency_threshold(self):
        if self._frequency_threshold is None:
            return ''
        else:
            return ('%s %d' % (self.__options__['frequency_threshold'],
                               self._frequency_threshold))

    def _makeconditional_probability_threshold(self):
        if self._conditional_probability_threshold is None:
            return ''
        else:
            return ('%s %.1f' %
                    (self.__options__['conditional_probability_threshold'],
                     self._conditional_probability_threshold))

    def _makeprefix_search(self):
        if self._prefix_search is None:
            return ''
        else:
            return ('%s %s' % (self.__options__['prefix_search'],
                               'yes' if self._prefix_search else 'no'))

    def _condition(self):
        return ('%(condition)s %(frequency_threshold)s ' \
                '%(conditional_probability_threshold)s %(prefix_search)s' % \
                dict(condition=QueryOptionsMixin._condition(self),
                     frequency_threshold=self._makefrequency_threshold(),
                     conditional_probability_threshold=
                         self._makeconditional_probability_threshold(),
                     prefix_search=self._makeprefix_search())).strip()

    def __str__(self):
        return 'suggest --table "%(table)s" --column "%(column)s" --types ' \
                '"%(types)s" %(condition)s --query "%(query)s"' % \
                dict(table=self._table.__tablename__,
                     column=self._table.kana.name,
                     types=self._types,
                     condition=self._condition(),
                     query=utils.escape(self._query))


class SuggestLoadQuery(LoadQuery):
    """'load' query for suggestion representation class"""

    def __str__(self):
        return 'load --table %(table)s --input_type json --each ' \
               '\'suggest_preparer(_id, type, item, sequence, time, ' \
               'pair_query)\' "%(data)s"' % dict(table=self._table.__name__,
                                                 data=self._makejson())
