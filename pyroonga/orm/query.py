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

    def all(self):
        """Obtain the all result from this query instance

        :returns: result of query as a Python's objects. (dict, list, etc...)
        """
        q = str(self)
        result = self._table.grn.query(q)
        return json.loads(result)

    def limit(self, lim):
        """Limit for number of result of query

        :param lim: max number of results.
        :returns: :class:`SelectQuery`\. for method chain.
        """
        self._limit = int(lim)
        return self

    def offset(self, off):
        """Offset for start position of result of query

        :param off: offset. Base is 0.
        :returns: :class:`SelectQuery`\ . for method chain.
        """
        self._offset = int(off)
        return self

    def sortby(self, *args):
        """Set the sort order for result of query

        :param args: :class:`pyroonga.orm.table.Column` of sort keys.
        :returns: :class:`SelectQuery`\ . for method chain.
        """
        self._sortby = args
        return self

    def output_columns(self, *args):
        """Select the output columns for result of query

        :param args: :class:`pyroonga.orm.table.Column`
        :returns: :class:`SelectQuery`\ . for method chain.
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

    def _makeparams(self):
        return ''

    def _condition(self):
        return '%(limit)s %(offset)s %(sortby)s %(output_columns)s ' \
               '%(params)s' % dict(limit=self._makelimit(),
                                   offset=self._makeoffset(),
                                   sortby=self._makesortby(),
                                   output_columns=self._makeoutput_columns(),
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
