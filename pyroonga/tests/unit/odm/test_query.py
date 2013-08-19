# -*- coding: utf-8 -*-

import random

import pytest

from pyroonga.odm import attributes, query, table

from pyroonga.tests import utils, mock


class TestGroongaRecord(object):
    def test___init___without_kwargs(self):
        record = query.GroongaRecord(None)
        assert isinstance(record, query.GroongaRecord)

    def test___init___with_class(self):
        class A(object):
            pass
        record = query.GroongaRecord(A)
        assert isinstance(record, query.GroongaRecord)

    def test___init___with_class_and_kwargs(self):
        class A(object):
            pass
        expected_attr = utils.random_string()
        setattr(A, expected_attr, 'test')
        record = query.GroongaRecord(A, **{expected_attr: 'test'})
        assert getattr(record, expected_attr) == 'test'

    def test___init___with_class_and_different_kwargs(self):
        class A(object):
            foo = None
            bar = None
        with pytest.raises(AttributeError):
            query.GroongaRecord(A, foo='bar', baz='baz')
        with pytest.raises(AttributeError):
            query.GroongaRecord(A, fo='bar', bar='baz')

    @pytest.mark.parametrize(('rv', 'expected'), (
        ('true', True),
        ('false', False),
    ))
    def test_delete_with_default(self, rv, expected):
        class A(object):
            __tablename__ = 'test_table_name'
            _id = None
            grn = mock.Mock()
        A.grn.query.return_value = rv
        record = query.GroongaRecord(A, _id='test_id')
        result = record.delete()
        assert result is expected

    def test_delete_with_immediate_is_True(self):
        class A(object):
            __tablename__ = 'test_table_name'
            _id = None
            grn = mock.Mock()
        A.grn.query.return_value = 'true'
        record = query.GroongaRecord(A, _id='test_id')
        result = record.delete(immediate=True)
        assert result is True

    def test_delete_with_immediate_is_False(self):
        class A(object):
            __tablename__ = 'test_table_name'
            _id = None
        record = query.GroongaRecord(A, _id='test_id')
        result = record.delete(immediate=False)
        assert isinstance(result, query.SimpleQuery)

    def test_commit_with_not_changed(self):
        class A(object):
            __tablename__ = 'test_table_name'
            foo = None
            grn = mock.Mock()
        A.grn.query.return_value = 2
        record = query.GroongaRecord(A, foo='hoge')
        result = record.commit()
        assert result == 0

    def test_commit(self):
        class A(object):
            __tablename__ = 'test_table_name'
            foo = None
            grn = mock.Mock()
        expected = random.randint(1, 100)
        A.grn.query.return_value = expected
        record = query.GroongaRecord(A, foo='hoge')
        object.__setattr__(record, '__dirty', True)
        result = record.commit()
        assert result == expected

    def test_asdict_with_no_attrs(self):
        record = query.GroongaRecord(None)
        assert record.asdict() == {}

    def test_asdict(self):
        class A(object):
            pass
        expected_attr = utils.random_string()
        expected_value = utils.random_string()
        setattr(A, expected_attr, None)
        record = query.GroongaRecord(A)
        setattr(record, expected_attr, expected_value)
        assert record.asdict() == {expected_attr: expected_value}

    def test_asdict_with_class(self):
        class A(object):
            foo = None
        record = query.GroongaRecord(A)
        assert record.asdict() == {}

    def test_asdict_with_class_and_kwargs(self):
        class A(object):
            foo = None
            bar = None
        record = query.GroongaRecord(A, foo='bar', bar='baz')
        assert record.asdict() == {'foo': 'bar', 'bar': 'baz'}

    @pytest.mark.parametrize(('excludes', 'expected'), (
        (['foo'], {'bar': 'baz', 'baz': 'hoge'}),
        (['bar'], {'foo': 'bar', 'baz': 'hoge'}),
        (['foo', 'baz'], {'bar': 'baz'}),
    ))
    def test_asdict_with_excludes(self, excludes, expected):
        class A(object):
            foo = None
            bar = None
            baz = None
        record = query.GroongaRecord(A, foo='bar', bar='baz', baz='hoge')
        result = record.asdict(excludes=excludes)
        assert result == expected

    def test___setattr___with_not_defined_attr(self):
        class A(object):
            pass
        record = query.GroongaRecord(A)
        with pytest.raises(AttributeError):
            record.missing = 'foo'

    def test___setattr__(self):
        class A(object):
            foo = None
        record = query.GroongaRecord(A, foo='bar')
        assert record.foo == 'bar'
        assert object.__getattribute__(record, '__dirty') is False
        expected = utils.random_string()
        record.foo = expected
        assert record.foo == expected
        assert object.__getattribute__(record, '__dirty') is True


class TestDrilldown(object):
    def test_default_class_attr(self):
        assert query.Drilldown._key is None
        assert query.Drilldown._nsubrecs is None

    def test___init___(self):
        drilldown = query.Drilldown('test1', 'test2')
        assert drilldown._key == 'test1'
        assert drilldown._nsubrecs == 'test2'


class TestSelectQuery(object):
    def make_match_column(self, name):
        c = mock.Mock(spec=table.Column)
        c.name = name
        return c

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
    def test_constant(self):
        assert (query.MatchColumn.operator == {
            query.Operator.OR: ' || ',
            query.Operator.MUL: ' * ',
        }) is True

    def test___init__(self):
        expr = query.MatchColumn('testvalue')
        assert (expr.value == 'testvalue') is True

    @pytest.mark.parametrize(('value', 'expected'), (
        ('', ''),
        ('testvalue', 'testvalue'),
        ('foo bar', 'foo bar'),
        ('foo bar baz', 'foo bar baz'),
        (10, '10'),
    ))
    def test___str__(self, value, expected):
        expr = query.MatchColumn(value)
        assert str(expr) == expected

    def test___eq__(self):
        expr = query.MatchColumn('v1')
        et = (expr == 'v2')
        assert (et.op == query.Operator.EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'v2'

    def test___ge__(self):
        expr = query.MatchColumn('v1')
        et = (expr >= 'expr2')
        assert (et.op == query.Operator.GREATER_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___gt__(self):
        expr = query.MatchColumn('v1')
        et = (expr > 'expr2')
        assert (et.op == query.Operator.GREATER_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___le__(self):
        expr = query.MatchColumn('v1')
        et = (expr <= 'expr2')
        assert (et.op == query.Operator.LESS_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___lt__(self):
        expr = query.MatchColumn('v1')
        et = (expr < 'expr2')
        assert (et.op == query.Operator.LESS_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___ne__(self):
        expr = query.MatchColumn('v1')
        et = (expr != 'expr2')
        assert (et.op == query.Operator.NOT_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___and__(self):
        expr = query.MatchColumn('v1')
        et = (expr & 'expr2')
        assert (et.op == query.Operator.AND) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___or__(self):
        expr = query.MatchColumn('v1')
        et = (expr | 'expr2')
        assert (et.op == query.Operator.OR) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___sub__(self):
        expr = query.MatchColumn('v1')
        et = (expr - 'expr2')
        assert (et.op == query.Operator.NOT) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___mul__(self):
        expr = query.MatchColumn('v1')
        et = (expr * 'expr2')
        assert (et.op == query.Operator.MUL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'


class TestOperator(object):
    @pytest.mark.parametrize(('attr', 'expected'), (
        ('EQUAL', 'EQUAL'),
        ('GREATER_EQUAL', 'GREATER_EQUAL'),
        ('GREATER_THAN', 'GREATER_THAN'),
        ('LESS_EQUAL', 'LESS_EQUAL'),
        ('LESS_THAN', 'LESS_THAN'),
        ('NOT_EQUAL', 'NOT_EQUAL'),
        ('OR', 'OR'),
        ('AND', 'AND'),
        ('NOT', 'NOT'),
        ('MUL', 'MUL'),
    ))
    def test_constant(self, attr, expected):
        result = object.__getattribute__(query.Operator, attr)
        assert (result == expected) is True


class TestExpression(object):
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
        ('foo bar', 'foo bar'),
        ('foo bar baz', 'foo bar baz'),
        (10, '10'),
    ))
    def test___str__(self, value, expected):
        expr = query.Expression(value)
        assert str(expr) == expected

    def test___eq__(self):
        expr = query.Expression('v1')
        et = (expr == 'v2')
        assert (et.op == query.Operator.EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'v2'

    def test___ge__(self):
        expr = query.Expression('v1')
        et = (expr >= 'expr2')
        assert (et.op == query.Operator.GREATER_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___gt__(self):
        expr = query.Expression('v1')
        et = (expr > 'expr2')
        assert (et.op == query.Operator.GREATER_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___le__(self):
        expr = query.Expression('v1')
        et = (expr <= 'expr2')
        assert (et.op == query.Operator.LESS_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___lt__(self):
        expr = query.Expression('v1')
        et = (expr < 'expr2')
        assert (et.op == query.Operator.LESS_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___ne__(self):
        expr = query.Expression('v1')
        et = (expr != 'expr2')
        assert (et.op == query.Operator.NOT_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___and__(self):
        expr = query.Expression('v1')
        et = (expr & 'expr2')
        assert (et.op == query.Operator.AND) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___or__(self):
        expr = query.Expression('v1')
        et = (expr | 'expr2')
        assert (et.op == query.Operator.OR) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___sub__(self):
        expr = query.Expression('v1')
        et = (expr - 'expr2')
        assert (et.op == query.Operator.NOT) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___mul__(self):
        expr = query.Expression('v1')
        et = (expr * 'expr2')
        assert (et.op == query.Operator.MUL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'


class TestQueryExpression(object):
    def test_constant(self):
        assert (query.QueryExpression.operator == {
            query.Operator.EQUAL: ':',
            query.Operator.GREATER_EQUAL: ':>=',
            query.Operator.GREATER_THAN: ':>',
            query.Operator.LESS_EQUAL: ':<=',
            query.Operator.LESS_THAN: ':<',
            query.Operator.NOT_EQUAL: ':!',
            query.Operator.OR: ' OR ',
            query.Operator.AND: ' + ',
            query.Operator.NOT: ' - ',
        }) is True

    def test___init__(self):
        expr = query.QueryExpression('testvalue')
        assert (expr.value == 'testvalue') is True

    @pytest.mark.parametrize(('value', 'expected'), (
        ('', ''),
        ('testvalue', 'testvalue'),
        ('foo bar', '"foo bar"'),
        ('foo bar baz', '"foo bar baz"'),
        (10, '10'),
    ))
    def test___str__(self, value, expected):
        expr = query.QueryExpression(value)
        assert str(expr) == expected

    def test___eq__(self):
        expr = query.QueryExpression('v1')
        et = (expr == 'v2')
        assert (et.op == query.Operator.EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'v2'

    def test___ge__(self):
        expr = query.QueryExpression('v1')
        et = (expr >= 'expr2')
        assert (et.op == query.Operator.GREATER_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___gt__(self):
        expr = query.QueryExpression('v1')
        et = (expr > 'expr2')
        assert (et.op == query.Operator.GREATER_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___le__(self):
        expr = query.QueryExpression('v1')
        et = (expr <= 'expr2')
        assert (et.op == query.Operator.LESS_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___lt__(self):
        expr = query.QueryExpression('v1')
        et = (expr < 'expr2')
        assert (et.op == query.Operator.LESS_THAN) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___ne__(self):
        expr = query.QueryExpression('v1')
        et = (expr != 'expr2')
        assert (et.op == query.Operator.NOT_EQUAL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___and__(self):
        expr = query.QueryExpression('v1')
        et = (expr & 'expr2')
        assert (et.op == query.Operator.AND) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___or__(self):
        expr = query.QueryExpression('v1')
        et = (expr | 'expr2')
        assert (et.op == query.Operator.OR) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___sub__(self):
        expr = query.QueryExpression('v1')
        et = (expr - 'expr2')
        assert (et.op == query.Operator.NOT) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'

    def test___mul__(self):
        expr = query.Expression('v1')
        et = (expr * 'expr2')
        assert (et.op == query.Operator.MUL) is True
        assert et.left is expr
        assert isinstance(et.right, query.Expression)
        assert et.right.value == 'expr2'


def test_GE():
    assert query.GE is query.Expression


class TestExpressionTree(object):
    @pytest.fixture
    def Expr(self):
        class C(object):
            def __init__(self, value):
                self.value = value

            def __str__(self):
                return str(self.value)
        return C

    def test___init__(self):
        et = query.ExpressionTree('testexpr', 'testleft', 'testright')
        assert (et.op == 'testexpr') is True
        assert isinstance(et.left, query.Expression)
        assert isinstance(et.right, query.Expression)

    def test___eq__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et == 'expr2')
        assert (et2.op == query.Operator.EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___ge__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et >= 'expr2')
        assert (et2.op == query.Operator.GREATER_EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___gt__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et > 'expr2')
        assert (et2.op == query.Operator.GREATER_THAN) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___le__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et <= 'expr2')
        assert (et2.op == query.Operator.LESS_EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___lt__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et < 'expr2')
        assert (et2.op == query.Operator.LESS_THAN) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___ne__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et != 'expr2')
        assert (et2.op == query.Operator.NOT_EQUAL) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___and__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et & 'expr2')
        assert (et2.op == query.Operator.AND) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___or__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et | 'expr2')
        assert (et2.op == query.Operator.OR) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test___sub__(self):
        et = query.ExpressionTree('expr1', 'left', 'right')
        et2 = (et - 'expr2')
        assert (et2.op == query.Operator.NOT) is True
        assert et2.left is et
        assert isinstance(et2.right, query.Expression)

    def test_build_with_missing_definition_of_expression(self, Expr):
        class A(Expr):
            operator = {'expr1': '|'}
        et = query.ExpressionTree('expr2', 'left1', 'right1')
        with pytest.raises(NotImplementedError):
            et.build(A)

    def test_build_without_nested(self, Expr):
        class A(Expr):
            operator = {'expr1': '|'}
        et = query.ExpressionTree('expr1', 'left1', 'right1')
        assert et.build(A) == '(left1|right1)'

    def test_build_with_nested(self, Expr):
        class A(Expr):
            operator = {
                '|': '|',
                '&': '&',
                '+': '+',
                }
        et1 = query.ExpressionTree('|', 'left1', 'right1')
        et2 = query.ExpressionTree('&', et1, 'right2')
        et3 = query.ExpressionTree('+', 'left3', et2)
        assert et3.build(A) == '(left3+((left1|right1)&right2))'


class TestSimpleQuery(object):
    @pytest.fixture
    def query(self):
        class A(object):
            __tablename__ = 'test_tablename'
        return query.SimpleQuery(A)

    def test___init__(self):
        class A(object):
            pass
        result = query.SimpleQuery(A)
        assert isinstance(result, query.SimpleQuery)

    @pytest.mark.parametrize(('key', 'id', 'filter', 'expected'), (
        ('test_key', 12, 'test_filter',
         'delete --table test_tablename --key test_key --id 12 --filter'
         ' "test_filter"',
         ),
        ('test_key', 'test_id', None,
         'delete --table test_tablename --key test_key --id test_id',
         ),
        ('test_key', None, 'test_filter',
         'delete --table test_tablename --key test_key --filter "test_filter"',
         ),
        (None, 'test_id', 'test_filter',
         'delete --table test_tablename --id test_id --filter "test_filter"',
         ),
        (None, None, 'test_filter',
         'delete --table test_tablename --filter "test_filter"',
         ),
        (None, 'test_id', None,
         'delete --table test_tablename --id test_id',
         ),
        ('test_key', None, None,
         'delete --table test_tablename --key test_key',
         ),
        (None, None, None, 'delete --table test_tablename'),
    ))
    def test_delete(self, query, key, id, filter, expected):
        result = query.delete(key=key, id=id, filter=filter)
        assert result is query
        assert str(result) == expected

    def test_truncate(self):
        expected = utils.random_string()

        class A(object):
            __tablename__ = expected
        q = query.SimpleQuery(A)
        result = q.truncate()
        assert result is q
        assert str(result) == 'truncate %s' % expected

    def test_cache_limit_with_param(self):
        class A(object):
            pass
        limit = random.randrange(2000)
        q = query.SimpleQuery(A)
        result = q.cache_limit(limit)
        assert result is q
        assert str(result) == 'cache_limit %s' % limit

    def test_cache_limit_without_param(self):
        class A(object):
            pass
        q = query.SimpleQuery(A)
        result = q.cache_limit()
        assert result is q
        assert str(result) == 'cache_limit'

    log_levels = [v for v in attributes.LogLevel.__dict__.values()
                  if isinstance(v, attributes.LogLevelSymbol)]

    @pytest.mark.parametrize('level', log_levels)
    def test_log_level(self, level):
        class A(object):
            pass
        q = query.SimpleQuery(A)
        result = q.log_level(level)
        assert result is q
        assert str(result) == 'log_level %s' % level

    @pytest.mark.parametrize('level', log_levels)
    def test_log_put(self, level):
        class A(object):
            pass
        msg = utils.random_string()
        q = query.SimpleQuery(A)
        result = q.log_put(level, msg)
        assert result is q
        assert str(result) == 'log_level %s %s' % (level, msg)

    def test_log_reopen(self):
        class A(object):
            pass
        q = query.SimpleQuery(A)
        result = q.log_reopen()
        assert result is q
        assert str(result) == 'log_reopen'

    @pytest.mark.parametrize(('ret', 'expected'), (
        ('[true]', [True]),
        ('[false]', [False]),
    ))
    def test_execute(self, ret, expected):
        class A(object):
            grn = mock.MagicMock()
        A.grn.query = mock.MagicMock(return_value=ret)
        record = query.SimpleQuery(A)
        result = record.execute()
        assert result == expected
        assert A.grn.query.mock_calls == [mock.call('')]
