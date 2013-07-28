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
