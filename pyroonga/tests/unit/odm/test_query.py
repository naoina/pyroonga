# -*- coding: utf-8 -*-

import random

import pytest

try:
    from unittest import mock
except ImportError:
    import mock

from pyroonga.odm import query, table


class TestSelectQuery(object):
    def make_match_column(self, name):
        c = mock.Mock(spec=table.Column)
        c.name = name
        return query.MatchColumn(c)

    def test_match_columns(self):
        q = query.SelectQuery(mock.MagicMock())
        result = q.match_columns()
        assert result is q

    def test_query(self):
        q = query.SelectQuery(mock.MagicMock())
        result = q.query()
        assert result is q

    def test___str__with_match_columns(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.match_columns(self.make_match_column('c1'))
        assert result is q
        assert q.__str__() == ('select --table "test_table" --match_columns'
                               " 'c1'")

    def test___str__with_match_columns_and_multiple_columns(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.match_columns(self.make_match_column('c1'),
                                 self.make_match_column('c2'))
        assert result is q
        assert q.__str__() == ('select --table "test_table" --match_columns'
                               " 'c1 || c2'")

    def test___str__with_query_and_no_args(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.query()
        assert result is q
        assert q.__str__() == ('select --table "test_table"')

    @pytest.mark.parametrize(('queries', 'expected'), (
        (('q1',), r'q1'),
        (('q1', 'q2'), r'q1 OR q2'),
        (('q1', 'q2', 'q3'), r'q1 OR q2 OR q3'),
        (('q1 q2',), r'\"q1 q2\"'),
        (('q1 q2', 'q3 q4'), r'\"q1 q2\" OR \"q3 q4\"'),
        ((query.GE('q1') | query.GE('q2'),), r'(q1 OR q2)'),
        ((query.GE('q1 q2') | query.GE('q3 q4'),),
         r'(\"q1 q2\" OR \"q3 q4\")'),
    ))
    def test___str__with_query_and_args(self, queries, expected):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.query(*queries)
        assert result is q
        assert q.__str__() == ('select --table "test_table" --query "%s"' %
                               expected)

    def test___str__with_query_and_kwargs(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.query(query1='q1', q1='query1')
        assert result is q
        assert q.__str__() == ('select --table "test_table" --query'
                               r' "(q1:@\"query1\" OR query1:@\"q1\")"')


class TestMatchColumn(object):
    def test_initial_weight_value(self):
        assert query.MatchColumn.INITIAL_WEIGHT == 1

    def test___init__with_default_value(self):
        expected = random.randrange(2000)
        c = query.MatchColumn(expected)
        assert c.column == expected
        assert c.weight == c.INITIAL_WEIGHT

    def test___init__with_weight(self):
        expected = random.randrange(2000)
        c = query.MatchColumn(None, expected)
        assert c.weight == expected

    def test___mul__(self):
        expected = random.randrange(2000)
        c = query.MatchColumn(None)
        assert c.weight == c.INITIAL_WEIGHT
        c2 = c.__mul__(expected)
        assert c.weight == c.INITIAL_WEIGHT
        assert c2.weight == expected

    def test___mul__by_implicit_calls(self):
        expected = random.randrange(2000)
        c = query.MatchColumn(None)
        assert c.weight == c.INITIAL_WEIGHT
        c2 = c * expected
        assert c.weight == c.INITIAL_WEIGHT
        assert c2.weight == expected

    def test___or__(self):
        expected = random.randrange(2000)
        c = query.MatchColumn(None)
        assert c.weight == c.INITIAL_WEIGHT
        c2 = c.__or__(expected)
        assert c.weight == c.INITIAL_WEIGHT
        assert isinstance(c2, query.MatchColumnsTree)
        assert c2.left is c
        assert c2.right == expected

    def test___or__with_implicit_calls(self):
        expected = random.randrange(2000)
        c = query.MatchColumn(None)
        assert c.weight == c.INITIAL_WEIGHT
        c2 = c | expected
        assert c.weight == c.INITIAL_WEIGHT
        assert isinstance(c2, query.MatchColumnsTree)
        assert c2.left is c
        assert c2.right == expected

    def test___str__without_weight(self):
        column = table.Column()
        column.name = 'test_name'
        c = query.MatchColumn(column)
        assert c.__str__() == 'test_name'

    def test___str__without_with_weight(self):
        column = table.Column()
        column.name = 'test_name'
        weight = random.randrange(2000)
        c = query.MatchColumn(column, weight)
        assert c.__str__() == 'test_name * %s' % weight


class TestMatchColumnsTree(object):
    def test___init__(self):
        expected_left = 'testleft'
        expected_right = 'testright'
        ct = query.MatchColumnsTree(expected_left, expected_right)
        assert ct.left == 'testleft'
        assert ct.right == 'testright'

    def test___or__(self):
        left = 'testleft'
        right = 'testright'
        expected_right = 'testright2'
        ct = query.MatchColumnsTree(left, right)
        ct2 = ct.__or__(expected_right)
        assert (ct.left, ct.right) == ('testleft', 'testright')
        assert ct2.left is ct
        assert ct2.right == 'testright2'

    def test___or__with_implicit_calls(self):
        left = 'testleft'
        right = 'testright'
        expected_right = 'testright2'
        ct = query.MatchColumnsTree(left, right)
        ct2 = ct | expected_right
        assert (ct.left, ct.right) == ('testleft', 'testright')
        assert ct2.left is ct
        assert ct2.right == 'testright2'

    def test___str__with_not_nested(self):
        oo = [table.Column() for _ in range(2)]
        for i, o in enumerate(oo):
            o.name = 'testcolumn%s' % i
        c1, c2 = [query.MatchColumn(o) for o in oo]
        ct = query.MatchColumnsTree(c1, c2)
        assert ct.__str__() == 'testcolumn0 || testcolumn1'

    def test___str__with_nested(self):
        oo = [table.Column() for _ in range(4)]
        for i, o in enumerate(oo):
            o.name = 'testcolumn%s' % i
        c1, c2, c3, c4 = [query.MatchColumn(o) for o in oo]
        ct = query.MatchColumnsTree(c1, c2)
        ct2 = query.MatchColumnsTree(ct, c3)
        ct3 = query.MatchColumnsTree(c4, ct2)
        assert ct3.__str__() == (
            'testcolumn3 || testcolumn0 || testcolumn1 || testcolumn2')

    @pytest.mark.parametrize(('left', 'right'), (
        (None, None),
        (None, 'right'),
        ('left', None),
        (1, 1),
    ))
    def test___str__with_invalid_value(self, left, right):
        with pytest.raises(ValueError):
            query.MatchColumnsTree(left, right).__str__()


class TestExpression(object):
    @pytest.mark.parametrize(('attr', 'expected'), (
        ('EQUAL', ':'),
        ('GREATER_EQUAL', ':>='),
        ('GREATER_THAN', ':>'),
        ('LESS_EQUAL', ':<='),
        ('LESS_THAN', ':<'),
        ('NOT_EQUAL', ':!'),
        ('OR', ' OR '),
        ('AND', ' + '),
        ('NOT', ' - '),
    ))
    def test_constant(self, attr, expected):
        result = object.__getattribute__(query.Expression, attr)
        assert (result == expected) is True

    def test___init__(self):
        expr = query.Expression('testvalue')
        assert (expr.value == 'testvalue') is True

    def test_wrap_expr_with_Expression_instances(self):
        expr1, expr2 = query.Expression('v1'), query.Expression('v2')
        exprs = tuple(query.Expression.wrap_expr(expr1, expr2))
        assert len(exprs) == 2
        assert exprs[0] is expr1
        assert exprs[1] is expr2

    def test_wrap_expr_with_ExpressionTree_instances(self):
        et1, et2 = (query.ExpressionTree('e1', 'l1', 'r1'),
                    query.ExpressionTree('e2', 'l2', 'r2'))
        exprs = tuple(query.Expression.wrap_expr(et1, et2))
        assert len(exprs) == 2
        assert exprs[0] is et1
        assert exprs[1] is et2

    def test_wrap_expr(self):
        exprs = tuple(query.Expression.wrap_expr('v1', 'v2', 10))
        assert len(exprs) == 3
        assert isinstance(exprs[0], query.Expression)
        assert isinstance(exprs[1], query.Expression)
        assert isinstance(exprs[2], query.Expression)
        assert (exprs[0].value == 'v1') is True
        assert (exprs[1].value == 'v2') is True
        assert (exprs[2].value == 10) is True

    @pytest.mark.parametrize(('value', 'expected'), (
        ('', ''),
        ('testvalue', 'testvalue'),
        ('foo bar', '"foo bar"'),
        ('foo bar baz', '"foo bar baz"'),
        (10, '10'),
    ))
    def test___str__(self, value, expected):
        expr = query.Expression(value)
        assert str(expr) == expected

    def test___eq__(self):
        expr = query.Expression('v1')
        et = (expr == 'v2')
        assert (et.expr == query.Expression.EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___ge__(self):
        expr = query.Expression('v1')
        et = (expr >= 'expr2')
        assert (et.expr == query.Expression.GREATER_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___gt__(self):
        expr = query.Expression('v1')
        et = (expr > 'expr2')
        assert (et.expr == query.Expression.GREATER_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___le__(self):
        expr = query.Expression('v1')
        et = (expr <= 'expr2')
        assert (et.expr == query.Expression.LESS_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___lt__(self):
        expr = query.Expression('v1')
        et = (expr < 'expr2')
        assert (et.expr == query.Expression.LESS_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___ne__(self):
        expr = query.Expression('v1')
        et = (expr != 'expr2')
        assert (et.expr == query.Expression.NOT_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___and__(self):
        expr = query.Expression('v1')
        et = (expr & 'expr2')
        assert (et.expr == query.Expression.AND) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___or__(self):
        expr = query.Expression('v1')
        et = (expr | 'expr2')
        assert (et.expr == query.Expression.OR) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)

    def test___sub__(self):
        expr = query.Expression('v1')
        et = (expr - 'expr2')
        assert (et.expr == query.Expression.NOT) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)


def test_GE():
    assert query.GE is query.Expression


class TestExpressionTree(object):
    def test___init__(self):
        et = query.ExpressionTree('testexpr', 'testleft', 'testright')
        assert (et.expr == 'testexpr') is True
        assert isinstance(et.left, query.Expression)
        assert isinstance(et.right, query.Expression)

    def test___eq__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et == 'expr2')
        assert (et2.expr == query.Expression.EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___ge__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et >= 'expr2')
        assert (et2.expr == query.Expression.GREATER_EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___gt__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et > 'expr2')
        assert (et2.expr == query.Expression.GREATER_THAN) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___le__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et <= 'expr2')
        assert (et2.expr == query.Expression.LESS_EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___lt__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et < 'expr2')
        assert (et2.expr == query.Expression.LESS_THAN) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___ne__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et != 'expr2')
        assert (et2.expr == query.Expression.NOT_EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___and__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et & 'expr2')
        assert (et2.expr == query.Expression.AND) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___or__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et | 'expr2')
        assert (et2.expr == query.Expression.OR) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___sub__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et - 'expr2')
        assert (et2.expr == query.Expression.NOT) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___str__without_nested(self):
        et = query.ExpressionTree('expr1', 'left1', 'right1')
        assert str(et) == '(left1expr1right1)'

    def test___str__with_nested(self):
        et1 = query.ExpressionTree('|', 'left1', 'right1')
        et2 = query.ExpressionTree('&', et1, 'right2')
        et3 = query.ExpressionTree('+', 'left3', et2)
        assert str(et3) == '(left3+((left1|right1)&right2))'
