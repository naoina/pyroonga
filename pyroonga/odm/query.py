# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
]

import itertools
import json
import logging
import operator
from functools import reduce

from pyroonga import utils
from pyroonga.odm.attributes import SuggestType

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
            :func:`pyroonga.odm.table.tablebase`\ .
        """
        self._table = tbl


class QueryOptionsMixin(object):
    __options__ = {
        'limit': '--limit',
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

        :param args: :class:`pyroonga.odm.table.Column` of sort keys.
        :returns: self. for method chain.
        """
        self._sortby = args
        return self

    def output_columns(self, *args):
        """Select the output columns for result of query

        :param args: :class:`pyroonga.odm.table.Column`
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
        return ' '.join((
            self._makelimit(),
            self._makeoffset(),
            self._makesortby(),
            self._makeoutput_columns()))


class GroongaResultBase(object):
    """Base class of query result"""

    def __init__(self, cls, results, maxlen=None):
        """Construct of GroongaResultBase

        :param cls: Class for mappings.
        :param results: query results.
        :param maxlen: maximum length of mapping results. Default is all.
        """
        self._result = [cls(**mapped) for mapped in
                        utils.to_python(results, 1, maxlen)]
        self._all_len = results[0][0]

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
        correct = result.get('correct', [])
        suggest = result.get('suggest', [])
        self.complete = complete and GroongaSuggestResult(Suggest, complete)
        self.correct = correct and GroongaSuggestResult(Suggest, correct)
        self.suggest = suggest and GroongaSuggestResult(Suggest, suggest)


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
        self._exprs = list(Expression.wrap_expr(*args))
        self._target = kwargs
        self._match_columns = []
        self._cache = True
        self._match_escalation_threshold = None

    def all(self):
        """Obtain the all result from this query instance

        :returns: result of query as a Python's objects. (dict, list, etc...)
        """
        q = str(self)
        result = self._table.grn.query(q)
        return GroongaSelectResult(self._table, result)

    def match_columns(self, *args):
        """Set the match columns

        :param args: iterable of columns
        :returns: self. for method chain
        """
        from pyroonga.odm.table import Column  # avoid Circular reference
        columns = (MatchColumn(c) if isinstance(c, Column) else c
                   for c in args)
        self._match_columns.extend(columns)
        return self

    def query(self, *args, **kwargs):
        """Set the query strings

        :param args: strings of query
        :param kwargs: search columns and search texts
        :returns: self. for method chain
        """
        self._exprs.extend(Expression.wrap_expr(*args))
        self._target.update(kwargs)
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

    def _make_match_columns(self):
        if self._match_columns:
            return "--match_columns '%s'" % reduce(operator.or_,
                                                   self._match_columns)
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
        return ' '.join((
            self._make_match_columns(),
            QueryOptionsMixin._condition(self),
            self._makecache(),
            self._makematch_escalation_threshold(),
            self._makeparams())).strip()

    def __str__(self):
        return ('select --table "%s" %s' % (
            self._table.__tablename__,
            self._condition())).strip()


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
        expr = Expression.OR.join(utils.escape(str(expr)) for expr in
                                  self._exprs)
        result = param and '(%s)' % param
        if result and expr:
            result += Expression.AND
        result += expr
        return '--query "%s"' % result if result else ''


class DrillDownQuery(SelectQueryBase, QueryOptionsMixin):
    """'select' query with drilldown representation class

    Instantiate from :meth:`SelectQuery.drilldown`\ .
    """

    __options__ = {
        'limit': '--drilldown_limit',
        'offset': '--drilldown_offset',
        'sortby': '--drilldown_sortby',
        'output_columns': '--drilldown_output_columns'}

    def __init__(self, parent, *args):
        """Construct of drilldown query

        :param parent: parent :class:`SelectQuery`\ .
        :param args: target columns for drilldown. Type is
            :class:`pyroonga.odm.table.Column`\ .
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


class MatchColumn(object):
    """match column representation class
    """
    INITIAL_WEIGHT = 1

    def __init__(self, column, weight=INITIAL_WEIGHT):
        self.column = column
        self.weight = weight

    def __mul__(self, other):
        return MatchColumn(self.column, int(other))

    def __or__(self, other):
        return MatchColumnsTree(self, other)

    def __str__(self):
        result = self.column.name
        if self.weight > MatchColumn.INITIAL_WEIGHT:
            result += ' * %s' % self.weight
        return result


class MatchColumnsTree(object):
    """Match columns tree"""

    __slots__ = ['left', 'right']

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __or__(self, other):
        return MatchColumnsTree(self, other)

    def _extract_tree(self, inst):
        if isinstance(inst, MatchColumnsTree):
            return '%s || %s' % (
                self._extract_tree(inst.left),
                self._extract_tree(inst.right))
        elif isinstance(inst, MatchColumn):
            return str(inst)
        else:
            raise ValueError(
                "instance takes only the `MatchColumn` and `MatchColumnsTree`."
                " but `%s` given." % repr(inst))

    def __str__(self):
        return self._extract_tree(self)


class BaseExpression(object):
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


class Expression(BaseExpression):
    """Expression constants"""

    __slots__ = ['value']

    EQUAL = ':'
    GREATER_EQUAL = ':>='
    GREATER_THAN = ':>'
    LESS_EQUAL = ':<='
    LESS_THAN = ':<'
    NOT_EQUAL = ':!'
    OR = ' OR '
    AND = ' + '
    NOT = ' - '

    def __init__(self, value):
        self.value = value

    @classmethod
    def wrap_expr(cls, *args):
        return (Expression(arg) if
                not isinstance(arg, (Expression, ExpressionTree))
                else arg for arg in args)

    def __str__(self):
        expr_str = str(self.value)
        return '"%s"' % expr_str if ' ' in expr_str else expr_str

GE = Expression


class ExpressionTree(BaseExpression):
    """Query conditional expression tree class"""

    __slots__ = ['expr', 'left', 'right']

    def __init__(self, expr, left, right):
        self.expr = expr
        self.left, self.right = tuple(Expression.wrap_expr(left, right))

    def __str__(self):
        return self._extract_tree(self)

    def _extract_tree(self, expr):
        if isinstance(expr, ExpressionTree):
            return '(%s%s%s)' % (
                self._extract_tree(expr.left), expr.expr,
                self._extract_tree(expr.right),
                )
        else:
            return str(expr)


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
        return ' '.join((
            'load',
            '--table', self._table.__name__,
            '--input-type', 'json',
            '--values', '"%s"' % self._makejson()))


class SuggestQuery(Query, QueryOptionsMixin):
    """'suggest' query representation class"""

    __options__ = {
        'limit': '--limit',
        'offset': '--offset',
        'sortby': '--sortby',
        'output_columns': '--output_columns',
        'frequency_threshold': '--frequency_threshold',
        'conditional_probability_threshold': (
            '--conditional_probability_threshold'),
        'prefix_search': '--prefix_search',
        'similar_search': '--similar_search'}

    def __init__(self, tbl, query):
        """Construct of 'suggest' query

        :param tbl: :class:`pyroonga.odm.table.item_query` class.
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
        self._similar_search = None
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

        :param types: see :class:`pyroonga.odm.attributes.SuggestType`
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

        :param isprefixsearch: It specifies whether optional prefix search is
            used or not in completion. 'yes' if True, otherwise 'no'.
        :returns: self. for method chain.  """
        self._prefix_search = bool(isprefixsearch)
        return self

    def similar_search(self, issimilar_search):
        """Set the similar search

        :param issimilar_search: It specifies whether optional similar search
            is used or not in completion. 'yes' if True, otherwise 'no'.
        :returns: self. for method chain.
        """
        self._similar_search = bool(issimilar_search)
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

    def _makesimilar_search(self):
        if self._similar_search is None:
            return ''
        else:
            return ('%s %s' % (self.__options__['similar_search'],
                               'yes' if self._similar_search else 'no'))

    def _condition(self):
        return ' '.join((
            QueryOptionsMixin._condition(self),
            self._makefrequency_threshold(),
            self._makeconditional_probability_threshold(),
            self._makeprefix_search(),
            self._makesimilar_search())).strip()

    def __str__(self):
        return ' '.join((
            'suggest',
            '--table', '"%s"' % self._table.__tablename__,
            '--column', '"%s"' % self._table.kana.name,
            '--types', '"%s"' % self._types,
            self._condition(),
            '--query', '"%s"' % utils.escape(self._query)))


class SuggestLoadQuery(LoadQuery):
    """'load' query for suggestion representation class"""

    def __str__(self):
        return ' '.join((
            'load',
            '--table', self._table.__name__,
            '--input_type', 'json',
            '--each',
            "'suggest_preparer(_id, type, item, sequence, time, pair_query)'",
            '"%s"' % self._makejson()))
