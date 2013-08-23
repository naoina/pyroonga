# -*- coding: utf-8 -*-

# Copyright (c) 2013 Naoya Inada <naoina@kuune.org>
# Licensed under the MIT License.

__author__ = "Naoya Inada <naoina@kuune.org>"

__all__ = [
]

import itertools
import json
import logging
from datetime import date, datetime

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


class GroongaRecord(object):
    def __init__(self, cls, **kwargs):
        """Construct of GroongaRecord

        :param kwargs: name and value of columns
        """
        object.__setattr__(self, '__cls', cls)
        object.__setattr__(self, '__dirty', False)
        for k, v in kwargs.items():
            try:
                object.__getattribute__(cls, k)
            except AttributeError:
                raise AttributeError('key "%s" is not defined in %s' %
                                     (k, cls.__name__))
            else:
                object.__setattr__(self, k, v)

    def delete(self, immediate=True):
        """Delete the record

        :param immediate: Delete the record immediately if True
        :returns: If ``immediate`` argument is True, True if successful,
            otherwise False. If ``immediate`` argument is False, It returns
            :class:`SimpleQuery` object for lazy execution
        """
        cls = object.__getattribute__(self, '__cls')
        query = SimpleQuery(cls).delete(id=self._id)
        return query.execute() if immediate else query

    def commit(self):
        """Load changed data to Groonga

        :returns: Number of changed data. but if data isn't changed, returns 0
        """
        if object.__getattribute__(self, '__dirty'):
            object.__setattr__(self, '__dirty', False)
            cls = object.__getattribute__(self, '__cls')
            return LoadQuery(cls, [self]).commit()
        else:
            return 0

    def asdict(self, excludes=tuple()):
        result = self.__dict__.copy()
        for attr in (tuple(excludes) + ('__cls', '__dirty')):
            result.pop(attr, None)
        return result

    def __setattr__(self, name, value):
        cls = object.__getattribute__(self, '__cls')
        try:
            object.__getattribute__(cls, name)
        except AttributeError:
            raise AttributeError('"%s" column is not defined in %s' %
                                 (name, cls.__name__))
        object.__setattr__(self, '__dirty', True)
        object.__setattr__(self, name, value)


class GroongaResultBase(object):
    """Base class of query result"""

    def __init__(self, cls, results, maxlen=None):
        """Construct of GroongaResultBase

        :param cls: Class for mappings.
        :param results: query results.
        :param maxlen: maximum length of mapping results. Default is all.
        """
        self._result = [GroongaRecord(cls, **mapped) for mapped in
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

    _key = None
    _nsubrecs = None

    def __init__(self, _key=None, _nsubrecs=None):
        self._key = _key
        self._nsubrecs = _nsubrecs


@utils.python_2_unicode_compatible
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
        self._filters = []
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
        columns = (Expression(getattr(c, 'name', c)) for c in args)
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

    def filter(self, *args, **kwargs):
        """Set the filter query parameters

        :param args: instances of :class:`ExpressionTree`
        :param kwargs: Column names and values for Match operator.
            If more than one given, they will be concatenate with *OR* operator
        :returns: self. for method chain
        """
        filters = self._filters
        filters.extend(Expression.wrap_expr(*args))
        filters.extend(Expression(k).match(v) for k, v in
                       sorted(kwargs.items()))
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
            exprs = (e.build(MatchColumn) for e in self._match_columns)
            result = MatchColumn.operator[Operator.OR].join(exprs)
            return '--match_columns %s' % utils.escape(result, True)
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

    def _makefilters(self):
        if not self._filters:
            return ''
        exprs = (e.build(FilterExpression) for e in self._filters)
        result = FilterExpression.operator[Operator.OR].join(exprs)
        return '--filter %s' % utils.escape(result, True)

    def _condition(self):
        return ' '.join((
            self._make_match_columns(),
            QueryOptionsMixin._condition(self),
            self._makecache(),
            self._makematch_escalation_threshold(),
            self._makeparams(),
            self._makefilters())).strip()

    def __str__(self):
        return utils.to_text('select --table %s %s' % (
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
        params = ['%s:@%s' % (k, utils.escape(v, True)) for k, v in
                  sorted(self._target.items())]
        op = QueryExpression.operator[Operator.BIT_OR]
        param = op.join(params)
        expr = op.join(expr.build(QueryExpression) for expr in self._exprs)
        result = param and '(%s)' % param
        if result and expr:
            result += QueryExpression.operator[Operator.BIT_AND]
        result += expr
        return '--query %s' % utils.escape(result, True) if result else ''


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


class Operator(object):
    EQUAL = 'EQUAL'
    GREATER_EQUAL = 'GREATER_EQUAL'
    GREATER_THAN = 'GREATER_THAN'
    LESS_EQUAL = 'LESS_EQUAL'
    LESS_THAN = 'LESS_THAN'
    NOT_EQUAL = 'NOT_EQUAL'
    OR = 'OR'
    AND = 'AND'
    NOT = 'NOT'
    DIFF = 'DIFF'
    INVERT = 'INVERT'
    BIT_AND = 'BIT_AND'
    BIT_OR = 'BIT_OR'
    BIT_XOR = 'BIT_XOR'
    LSHIFT = 'LSHIFT'
    RSHIFT = 'RSHIFT'
    ADD = 'ADD'
    SUB = 'SUB'
    MUL = 'MUL'
    DIV = 'DIV'
    MOD = 'MOD'
    IADD = 'IADD'
    ISUB = 'ISUB'
    IMUL = 'IMUL'
    IDIV = 'IDIV'
    IMOD = 'IMOD'
    ILSHIFT = 'ILSHIFT'
    IRSHIFT = 'IRSHIFT'
    IBIT_AND = 'IBIT_AND'
    IBIT_OR = 'IBIT_OR'
    IBIT_XOR = 'IBIT_XOR'
    MATCH = 'MATCH'
    STARTSWITH = 'STARTSWITH'
    ENDSWITH = 'ENDSWITH'
    NEAR = 'NEAR'
    SIMILAR = 'SIMILAR'
    TERM_EXTRACT = 'TERM_EXTRACT'


class BaseExpression(object):
    __slots__ = []

    @property
    def lvalue(self):
        return self

    def __eq__(self, other):
        return ExpressionTree(Operator.EQUAL, self.lvalue, other)

    def __ge__(self, other):
        return ExpressionTree(Operator.GREATER_EQUAL, self.lvalue, other)

    def __gt__(self, other):
        return ExpressionTree(Operator.GREATER_THAN, self.lvalue, other)

    def __le__(self, other):
        return ExpressionTree(Operator.LESS_EQUAL, self.lvalue, other)

    def __lt__(self, other):
        return ExpressionTree(Operator.LESS_THAN, self.lvalue, other)

    def __ne__(self, other):
        return ExpressionTree(Operator.NOT_EQUAL, self.lvalue, other)

    def __add__(self, other):
        return ExpressionTree(Operator.ADD, self.lvalue, other)

    def __sub__(self, other):
        return ExpressionTree(Operator.SUB, self.lvalue, other)

    def __mul__(self, other):
        return ExpressionTree(Operator.MUL, self.lvalue, other)

    def __truediv__(self, other):
        return ExpressionTree(Operator.DIV, self.lvalue, other)

    def __div__(self, other):
        return self.__truediv__(other)

    def __mod__(self, other):
        return ExpressionTree(Operator.MOD, self.lvalue, other)

    def not_(self):
        return ExpressionTree(Operator.NOT, None, self.lvalue)

    def and_(self, other):
        return ExpressionTree(Operator.AND, self.lvalue, other)

    def or_(self, other):
        return ExpressionTree(Operator.OR, self.lvalue, other)

    def diff(self, other):
        return ExpressionTree(Operator.DIFF, self.lvalue, other)

    def __invert__(self):
        return ExpressionTree(Operator.INVERT, None, self.lvalue)

    def __and__(self, other):
        return ExpressionTree(Operator.BIT_AND, self.lvalue, other)

    def __or__(self, other):
        return ExpressionTree(Operator.BIT_OR, self.lvalue, other)

    def __xor__(self, other):
        return ExpressionTree(Operator.BIT_XOR, self.lvalue, other)

    def __lshift__(self, other):
        return ExpressionTree(Operator.LSHIFT, self.lvalue, other)

    def __rshift__(self, other):
        return ExpressionTree(Operator.RSHIFT, self.lvalue, other)

    def __iadd__(self, other):
        return ExpressionTree(Operator.IADD, self.lvalue, other)

    def __isub__(self, other):
        return ExpressionTree(Operator.ISUB, self.lvalue, other)

    def __imul__(self, other):
        return ExpressionTree(Operator.IMUL, self.lvalue, other)

    def __itruediv__(self, other):
        return ExpressionTree(Operator.IDIV, self.lvalue, other)

    def __idiv__(self, other):
        return self.__itruediv__(other)

    def __imod__(self, other):
        return ExpressionTree(Operator.IMOD, self.lvalue, other)

    def __ilshift__(self, other):
        return ExpressionTree(Operator.ILSHIFT, self.lvalue, other)

    def __irshift__(self, other):
        return ExpressionTree(Operator.IRSHIFT, self.lvalue, other)

    def __iand__(self, other):
        return ExpressionTree(Operator.IBIT_AND, self.lvalue, other)

    def __ior__(self, other):
        return ExpressionTree(Operator.IBIT_OR, self.lvalue, other)

    def __ixor__(self, other):
        return ExpressionTree(Operator.IBIT_XOR, self.lvalue, other)

    def match(self, other):
        return ExpressionTree(Operator.MATCH, self.lvalue, other)

    def startswith(self, other):
        return ExpressionTree(Operator.STARTSWITH, self.lvalue, other)

    def endswith(self, other):
        return ExpressionTree(Operator.ENDSWITH, self.lvalue, other)

    def near(self, other):
        return ExpressionTree(Operator.NEAR, self.lvalue, other)

    def similar(self, other):
        return ExpressionTree(Operator.SIMILAR, self.lvalue, other)

    def term_extract(self, other):
        return ExpressionTree(Operator.TERM_EXTRACT, self.lvalue, other)


@utils.python_2_unicode_compatible
class Expression(BaseExpression):
    """Expression constants"""

    __slots__ = ['value']

    def __init__(self, value):
        super(Expression, self).__init__()
        self.value = value

    @classmethod
    def wrap_expr(cls, *args):
        return (cls._wrap(e) for e in args)

    @classmethod
    def _wrap(cls, expr):
        if expr is None or isinstance(expr, (Expression, ExpressionTree)):
            return expr
        return Expression(expr)

    def build(self, expr_cls):
        return str(expr_cls(self))

    def __str__(self):
        return utils.to_text(self.value)


class MatchColumn(Expression):
    """Match column class"""

    operator = {
        Operator.OR: ' || ',
        Operator.MUL: ' * ',
        }


@utils.python_2_unicode_compatible
class QueryExpression(Expression):
    """Query expression class"""

    operator = {
        Operator.EQUAL: ':',
        Operator.GREATER_EQUAL: ':>=',
        Operator.GREATER_THAN: ':>',
        Operator.LESS_EQUAL: ':<=',
        Operator.LESS_THAN: ':<',
        Operator.NOT_EQUAL: ':!',
        Operator.BIT_OR: ' OR ',
        Operator.BIT_AND: ' + ',
        Operator.SUB: ' - ',
        }

    def __str__(self):
        expr_str = utils.to_text(self.value)
        return utils.escape(expr_str)


@utils.python_2_unicode_compatible
class FilterExpression(Expression):
    """Filter expression class"""

    operator = {
        Operator.ADD: ' + ',
        Operator.SUB: ' - ',
        Operator.MUL: ' * ',
        Operator.DIV: ' / ',
        Operator.MOD: ' % ',
        Operator.NOT: '!',
        Operator.AND: ' && ',
        Operator.OR: ' || ',
        Operator.DIFF: ' &! ',
        Operator.INVERT: '~',
        Operator.BIT_AND: ' & ',
        Operator.BIT_OR: ' | ',
        Operator.BIT_XOR: ' ^ ',
        Operator.LSHIFT: ' << ',
        Operator.RSHIFT: ' >>> ',
        Operator.EQUAL: ' == ',
        Operator.NOT_EQUAL: ' != ',
        Operator.LESS_THAN: ' < ',
        Operator.LESS_EQUAL: ' <= ',
        Operator.GREATER_THAN: ' > ',
        Operator.GREATER_EQUAL: ' >= ',
        Operator.IADD: ' += ',
        Operator.ISUB: ' -= ',
        Operator.IMUL: ' *= ',
        Operator.IDIV: ' /= ',
        Operator.IMOD: ' %= ',
        Operator.ILSHIFT: ' <<= ',
        Operator.IRSHIFT: ' >>>= ',
        Operator.IBIT_AND: ' &= ',
        Operator.IBIT_OR: ' |= ',
        Operator.IBIT_XOR: ' ^= ',
        Operator.MATCH: ' @ ',
        Operator.STARTSWITH: ' @^ ',
        Operator.ENDSWITH: ' @$ ',
        Operator.NEAR: ' *N ',
        Operator.SIMILAR: ' *S ',
        Operator.TERM_EXTRACT: ' *T ',
        }

    def __str__(self):
        value = self.value
        if value is True:
            expr_str = 'true'
        elif value is False:
            expr_str = 'false'
        elif value is None:
            expr_str = 'null'
        elif isinstance(value, (datetime, date)):
            expr_str = utils.escape(value.strftime('%Y/%m/%d %H:%M:%S.%f'))
        else:
            expr_str = utils.escape(utils.to_text(value))
        # TODO: Geo point, Array
        return expr_str

GE = Expression


class ExpressionTree(BaseExpression):
    """Query conditional expression tree class"""

    __slots__ = ['op', 'left', 'right']

    def __init__(self, op, left, right):
        super(ExpressionTree, self).__init__()
        self.op = op
        self.left, self.right = tuple(Expression.wrap_expr(left, right))

    def build(self, expr_cls):
        return self._extract_tree(self, expr_cls)

    def _extract_tree(self, expr, expr_cls):
        if expr is None:
            return ''
        if not isinstance(expr, ExpressionTree):
            return expr_cls(expr)
        op = expr_cls.operator.get(expr.op, None)
        if op is None:
            raise NotImplementedError(
                "An operator `%s` is not defined in `%s`" %
                (expr.op, expr_cls.__name__))
        return '(%s%s%s)' % (
            self._extract_tree(expr.left, expr_cls), op,
            self._extract_tree(expr.right, expr_cls),
            )


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
        return json.dumps([v.asdict(excludes=('_id',)) for v in self._data])

    def __str__(self):
        return ' '.join((
            'load',
            '--table', self._table.__tablename__,
            '--input-type', 'json',
            '--values', utils.escape(self._makejson(), True)))


class SimpleQuery(Query):
    """simple true or false returning query representation class"""

    def __init__(self, table_cls):
        """Constructor of SimpleQuery

        :param table_cls: Class of Table
        """
        super(SimpleQuery, self).__init__(table_cls)
        self._query = []

    def delete(self, key=None, id=None, filter=None):
        """Get the 'delete' query

        :param table: Table class
        :param key: string of key of record, default is None
        :param id: string of id of record, default is None
        :param filter: instance of ExpressionTree or string of filter,
            default is None
        :returns: :class:`SimpleQuery` object
        """
        query = ['delete', '--table', self._table.__tablename__]
        if key is not None:
            query.extend(('--key', key))
        if id is not None:
            query.extend(('--id', str(id)))
        if filter is not None:
            query.extend(('--filter', utils.escape(str(filter), True)))
        self._query.extend(query)
        return self

    def truncate(self):
        """Get the 'truncate' query

        :returns: :class:`SimpleQuery` object
        """
        self._query = ['truncate', self._table.__tablename__]
        return self

    def cache_limit(self, max_limit=None):
        """Get the 'cache_limit' query

        :param max_limit: max number of cache limit to set.
            If None, It returns query of returns current value of cache limit.
            this behavior is same as Groonga
        :returns: :class:`SimpleQuery` object
        """
        self._query = ['cache_limit']
        if max_limit is not None:
            self._query.append(str(max_limit))
        return self

    def log_level(self, level):
        """Get the 'log_level' query

        :param level: Log output level. Values are defined in
            :class:`pyroonga.odm.attributes.LogLevel`
        :returns: :class:`SimpleQuery` object
        """
        self._query = ['log_level', str(level)]
        return self

    def log_put(self, level, message):
        """Get the 'log_put' query

        :param level: Log output level. Values are defined in
            :class:`pyroonga.odm.attributes.LogLevel`
        :param message: string of log for output
        :returns: :class:`SimpleQuery` object
        """
        self._query = ['log_level', str(level), message]
        return self

    def log_reopen(self):
        """Get the 'log_reopen' query

        :returns: :class:`SimpleQuery` object
        """
        self._query = ['log_reopen']
        return self

    def execute(self):
        """execute a query

        :returns: True if query is successful, otherwise False
        """
        return json.loads(self._table.grn.query(str(self)))

    def __str__(self):
        return ' '.join(self._query)


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
            '--query', utils.escape(self._query, True)))


class SuggestLoadQuery(LoadQuery):
    """'load' query for suggestion representation class"""

    def __str__(self):
        return ' '.join((
            'load',
            '--table', self._table.__tablename__,
            '--input_type', 'json',
            '--each',
            "'suggest_preparer(_id, type, item, sequence, time, pair_query)'",
            utils.escape(self._makejson(), True)))
