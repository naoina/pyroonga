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


class SelectQuery(Query):
    """Query representation class for 'select' query"""

    def __init__(self, tbl, *args, **kwargs):
        """Construct of 'select' query

        :param tbl: Table class. see also :class:`Query`\ .
        :param args: :class:`ExpressionTree`\ .
        :param kwargs: search columns and search texts.
        """
        super(SelectQuery, self).__init__(tbl)
        self._expr = args
        self._target = kwargs

    def all(self):
        """Obtain the all result from this query instance

        :returns: result of query as a Python's objects. (dict, list, etc...)
        """
        q = str(self)
        result = self._table.grn.query(q)
        return json.loads(result)

    def _makeparam(self):
        params = [utils.escape('%s:@%s' % target) for target in
                  self._target.items()]
        param = Expression.OR.join(params)
        exprs = [utils.escape(self._makeexpr(expr)) for expr in self._expr]
        expr = Expression.OR.join(exprs)
        result = param and '(%s)' % param
        if result and expr:
            result += Expression.AND
        result += expr
        return result

    def _makeexpr(self, expr):
        if isinstance(expr, ExpressionTree):
            return '(%s%s%s)' % (self._makeexpr(expr.left), expr.expr,
                    self._makeexpr(expr.right))
        else:
            return str(expr)

    def __str__(self):
        return 'select --table "%s" --query "%s"' % (self._table.__tablename__,
                self._makeparam())


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