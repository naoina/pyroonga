# -*- coding: utf-8 -*-

import random
from datetime import date, datetime

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

    def test_filter(self):
        q = query.SelectQuery(mock.MagicMock())
        result = q.filter()
        assert result is q

    def test___str__with_match_columns(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.match_columns(self.make_match_column('c1'))
        assert result is q
        assert str(q) == ('select --table test_table --match_columns "c1"')

    def test___str__with_match_columns_and_multiple_columns(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.match_columns(self.make_match_column('c1'),
                                 self.make_match_column('c2'))
        assert result is q
        assert str(q) == ('select --table test_table --match_columns'
                          ' "c1 || c2"')

    def test___str__with_query_and_no_args(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.query()
        assert result is q
        assert str(q) == ('select --table test_table')

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
        assert str(q) == ('select --table test_table --query "%s"' % expected)

    def test___str__with_query_and_kwargs(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.query(query1='q1', q1='query1')
        assert result is q
        assert str(q) == ('select --table test_table --query'
                          r' "(q1:@\"query1\" OR query1:@\"q1\")"')

    def test___str___with_filter_and_no_args(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.filter()
        assert result is q
        assert str(q) == ('select --table test_table')

    @pytest.mark.parametrize(('filters', 'expected'), (
        ([query.GE('q1')], 'q1'),
        ([query.GE('"q1"')], r'\\\"q1\\\"'),
        ([query.GE(r'\"q1\\"q2\"')], r'\\\\\\\"q1\\\\\\\\\\\"q2\\\\\\\"'),
        ([query.GE('"q1 q2"')], r'\"\\\"q1 q2\\\"\"'),
        ([query.GE('q1'), query.GE('q2')], 'q1 || q2'),
        ([query.GE('q1'), query.GE('q2'), query.GE('q3')], 'q1 || q2 || q3'),
        ([query.GE('q1 q2')], r'\"q1 q2\"'),
        ([query.GE('q1 q2'), query.GE('q3 q4')], r'\"q1 q2\" || \"q3 q4\"'),
        ([query.GE('q1') + query.GE('q2')], '(q1 + q2)'),
        ([query.GE('q1') + 'q2'], '(q1 + q2)'),
        ([query.GE('q1 q2') + query.GE('q3 q4')], r'(\"q1 q2\" + \"q3 q4\")'),
        ([query.GE('q1 q2') + 'q3 q4'], r'(\"q1 q2\" + \"q3 q4\")'),
        ([query.GE('q1') - query.GE('q2')], '(q1 - q2)'),
        ([query.GE('q1') - 'q2'], '(q1 - q2)'),
        ([query.GE('q1 q2') - query.GE('q3 q4')], r'(\"q1 q2\" - \"q3 q4\")'),
        ([query.GE('q1 q2') - 'q3 q4'], r'(\"q1 q2\" - \"q3 q4\")'),
        ([query.GE('q1') * query.GE('q2')], '(q1 * q2)'),
        ([query.GE('q1') * 'q2'], '(q1 * q2)'),
        ([query.GE('q1 q2') * query.GE('q3 q4')], r'(\"q1 q2\" * \"q3 q4\")'),
        ([query.GE('q1 q2') * 'q3 q4'], r'(\"q1 q2\" * \"q3 q4\")'),
        ([query.GE('q1') / query.GE('q2')], '(q1 / q2)'),
        ([query.GE('q1') / 'q2'], '(q1 / q2)'),
        ([query.GE('q1 q2') / 'q3 q4'], r'(\"q1 q2\" / \"q3 q4\")'),
        ([query.GE('q1') % query.GE('q2')], '(q1 % q2)'),
        ([query.GE('q1') % 'q2'], '(q1 % q2)'),
        ([query.GE('q1 q2') % 'q3 q4'], r'(\"q1 q2\" % \"q3 q4\")'),
        ([query.GE('q1').not_()], '(!q1)'),
        ([query.GE('q1').not_().not_()], '(!(!q1))'),
        ([query.GE('q1 q2').not_()], r'(!\"q1 q2\")'),
        ([(query.GE('q1 q2') == query.GE('q3 q4')).not_()],
         r'(!(\"q1 q2\" == \"q3 q4\"))'),
        ([query.GE('q1').and_('q2')], '(q1 && q2)'),
        ([query.GE('q1 q2').and_('q3 q4')], r'(\"q1 q2\" && \"q3 q4\")'),
        ([query.GE('q1').or_('q2')], '(q1 || q2)'),
        ([query.GE('q1 q2').or_('q3 q4')], r'(\"q1 q2\" || \"q3 q4\")'),
        ([query.GE('q1').diff('q2')], '(q1 &! q2)'),
        ([query.GE('q1 q2').diff('q3 q4')], r'(\"q1 q2\" &! \"q3 q4\")'),
        ([~query.GE('q1')], '(~q1)'),
        ([~~query.GE('q1')], '(~(~q1))'),
        ([~query.GE('q1 q2')], r'(~\"q1 q2\")'),
        ([~(query.GE('q1 q2') == query.GE('q3 q4'))],
         r'(~(\"q1 q2\" == \"q3 q4\"))'),
        ([query.GE('q1') & query.GE('q2')], '(q1 & q2)'),
        ([query.GE('q1') & 'q2'], '(q1 & q2)'),
        ([query.GE('q1 q2') & query.GE('q3 q4')], r'(\"q1 q2\" & \"q3 q4\")'),
        ([query.GE('q1 q2') & 'q3 q4'], r'(\"q1 q2\" & \"q3 q4\")'),
        ([query.GE('q1') | query.GE('q2')], '(q1 | q2)'),
        ([query.GE('q1') | 'q2'], '(q1 | q2)'),
        ([query.GE('q1 q2') | query.GE('q3 q4')], r'(\"q1 q2\" | \"q3 q4\")'),
        ([query.GE('q1 q2') | 'q3 q4'], r'(\"q1 q2\" | \"q3 q4\")'),
        ([query.GE('q1') ^ query.GE('q2')], '(q1 ^ q2)'),
        ([query.GE('q1') ^ 'q2'], '(q1 ^ q2)'),
        ([query.GE('q1 q2') ^ query.GE('q3 q4')], r'(\"q1 q2\" ^ \"q3 q4\")'),
        ([query.GE('q1 q2') ^ 'q3 q4'], r'(\"q1 q2\" ^ \"q3 q4\")'),
        ([query.GE('q1') << query.GE('q2')], '(q1 << q2)'),
        ([query.GE('q1') << 'q2'], '(q1 << q2)'),
        ([query.GE('q1 q2') << query.GE('q3 q4')], r'(\"q1 q2\" << \"q3 q4\")'),
        ([query.GE('q1 q2') << 'q3 q4'], r'(\"q1 q2\" << \"q3 q4\")'),
        ([query.GE('q1') >> query.GE('q2')], '(q1 >>> q2)'),
        ([query.GE('q1') >> 'q2'], '(q1 >>> q2)'),
        ([query.GE('q1 q2') >> query.GE('q3 q4')], r'(\"q1 q2\" >>> \"q3 q4\")'),
        ([query.GE('q1 q2') >> 'q3 q4'], r'(\"q1 q2\" >>> \"q3 q4\")'),
        ([query.GE('q1') == query.GE('q2')], '(q1 == q2)'),
        ([query.GE('q1') == 'q2'], '(q1 == q2)'),
        ([query.GE('q1 q2') == 'q3 q4'], r'(\"q1 q2\" == \"q3 q4\")'),
        ([query.GE('q1') != query.GE('q2')], '(q1 != q2)'),
        ([query.GE('q1') != 'q2'], '(q1 != q2)'),
        ([query.GE('q1 q2') != 'q3 q4'], r'(\"q1 q2\" != \"q3 q4\")'),
        ([query.GE('q1') < query.GE('q2')], '(q1 < q2)'),
        ([query.GE('q1') < 'q2'], '(q1 < q2)'),
        ([query.GE('q1 q2') < query.GE('q3 q4')], r'(\"q1 q2\" < \"q3 q4\")'),
        ([query.GE('q1 q2') < 'q3 q4'], r'(\"q1 q2\" < \"q3 q4\")'),
        ([query.GE('q1') <= query.GE('q2')], '(q1 <= q2)'),
        ([query.GE('q1') <= 'q2'], '(q1 <= q2)'),
        ([query.GE('q1 q2') <= query.GE('q3 q4')], r'(\"q1 q2\" <= \"q3 q4\")'),
        ([query.GE('q1 q2') <= 'q3 q4'], r'(\"q1 q2\" <= \"q3 q4\")'),
        ([query.GE('q1') > query.GE('q2')], '(q1 > q2)'),
        ([query.GE('q1') > 'q2'], '(q1 > q2)'),
        ([query.GE('q1 q2') > query.GE('q3 q4')], r'(\"q1 q2\" > \"q3 q4\")'),
        ([query.GE('q1 q2') > 'q3 q4'], r'(\"q1 q2\" > \"q3 q4\")'),
        ([query.GE('q1') >= query.GE('q2')], '(q1 >= q2)'),
        ([query.GE('q1') >= 'q2'], '(q1 >= q2)'),
        ([query.GE('q1 q2') >= query.GE('q3 q4')], r'(\"q1 q2\" >= \"q3 q4\")'),
        ([query.GE('q1 q2') >= 'q3 q4'], r'(\"q1 q2\" >= \"q3 q4\")'),
        ([query.GE('q1').match('q2')], '(q1 @ q2)'),
        ([query.GE('q1 q2').match('q3 q4')], r'(\"q1 q2\" @ \"q3 q4\")'),
        ([query.GE('q1').startswith('q2')], '(q1 @^ q2)'),
        ([query.GE('q1 q2').startswith('q3 q4')], r'(\"q1 q2\" @^ \"q3 q4\")'),
        ([query.GE('q1').endswith('q2')], '(q1 @$ q2)'),
        ([query.GE('q1 q2').endswith('q3 q4')], r'(\"q1 q2\" @$ \"q3 q4\")'),
        ([query.GE('q1').near('q2')], '(q1 *N q2)'),
        ([query.GE('q1 q2').near('q3 q4')], r'(\"q1 q2\" *N \"q3 q4\")'),
        ([query.GE('q1').similar('q2')], '(q1 *S q2)'),
        ([query.GE('q1 q2').similar('q3 q4')], r'(\"q1 q2\" *S \"q3 q4\")'),
        ([query.GE('q1').term_extract('q2')], '(q1 *T q2)'),
        ([query.GE('q1 q2').term_extract('q3 q4')],
         r'(\"q1 q2\" *T \"q3 q4\")'),
    ))
    def test___str___with_filter_and_args(self, filters, expected):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.filter(*filters)
        assert result is q
        assert str(q) == (r'select --table test_table --filter "%s"' %
                          expected)

    def test___str___with_filter_and_kwargs(self):
        m = mock.MagicMock()
        m.__tablename__ = 'test_table'
        q = query.SelectQuery(m)
        result = q.filter(filter1='f1 f2', f1='filter1')
        assert result is q
        assert str(q) == ('select --table test_table --filter'
                          r' "(f1 @ filter1) || (filter1 @ \"f1 f2\")"')


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
        ('DIFF', 'DIFF'),
        ('INVERT', 'INVERT'),
        ('BIT_AND', 'BIT_AND'),
        ('BIT_OR', 'BIT_OR'),
        ('BIT_XOR', 'BIT_XOR'),
        ('LSHIFT', 'LSHIFT'),
        ('RSHIFT', 'RSHIFT'),
        ('ADD', 'ADD'),
        ('SUB', 'SUB'),
        ('MUL', 'MUL'),
        ('DIV', 'DIV'),
        ('MOD', 'MOD'),
        ('IADD', 'IADD'),
        ('ISUB', 'ISUB'),
        ('IMUL', 'IMUL'),
        ('IDIV', 'IDIV'),
        ('IMOD', 'IMOD'),
        ('ILSHIFT', 'ILSHIFT'),
        ('IRSHIFT', 'IRSHIFT'),
        ('IBIT_AND', 'IBIT_AND'),
        ('IBIT_OR', 'IBIT_OR'),
        ('IBIT_XOR', 'IBIT_XOR'),
        ('MATCH', 'MATCH'),
        ('STARTSWITH', 'STARTSWITH'),
        ('ENDSWITH', 'ENDSWITH'),
        ('NEAR', 'NEAR'),
        ('SIMILAR', 'SIMILAR'),
        ('TERM_EXTRACT', 'TERM_EXTRACT'),
    ))
    def test_constant(self, attr, expected):
        result = object.__getattribute__(query.Operator, attr)
        assert (result == expected) is True


class BaseTestExpression(object):
    @pytest.fixture
    def Expression(self):
        raise NotImplementedError

    @pytest.fixture
    def expr(self, Expression):
        return Expression()

    @pytest.fixture
    def random_string(self):
        return utils.random_string()

    def test_lvalue(self, expr):
        assert expr.lvalue is expr

    def _test_op(self, left, result, expected, op):
        assert (result.op == op) is True
        assert result.left is left
        assert isinstance(result.right, query.Expression)
        assert (result.right.value == expected) is True

    def test___eq__(self, expr, random_string):
        et = (expr == random_string)
        self._test_op(expr, et, random_string, query.Operator.EQUAL)

    def test___ge__(self, expr, random_string):
        et = (expr >= random_string)
        self._test_op(expr, et, random_string, query.Operator.GREATER_EQUAL)

    def test___gt__(self, expr, random_string):
        et = (expr > random_string)
        self._test_op(expr, et, random_string, query.Operator.GREATER_THAN)

    def test___le__(self, expr, random_string):
        et = (expr <= random_string)
        self._test_op(expr, et, random_string, query.Operator.LESS_EQUAL)

    def test___lt__(self, expr, random_string):
        et = (expr < random_string)
        self._test_op(expr, et, random_string, query.Operator.LESS_THAN)

    def test___ne__(self, expr, random_string):
        et = (expr != random_string)
        self._test_op(expr, et, random_string, query.Operator.NOT_EQUAL)

    def test___add__(self, expr, random_string):
        et = (expr + random_string)
        self._test_op(expr, et, random_string, query.Operator.ADD)

    def test___sub__(self, expr, random_string):
        et = (expr - random_string)
        self._test_op(expr, et, random_string, query.Operator.SUB)

    def test___mul__(self, expr, random_string):
        et = (expr * random_string)
        self._test_op(expr, et, random_string, query.Operator.MUL)

    def test___div__(self, expr, random_string):
        et = (expr / random_string)
        self._test_op(expr, et, random_string, query.Operator.DIV)

    def test___mod__(self, expr, random_string):
        et = (expr % random_string)
        self._test_op(expr, et, random_string, query.Operator.MOD)

    def test_not_(self, expr):
        et = expr.not_()
        self._test_op(None, et, expr.value, query.Operator.NOT)

    def test_and_(self, expr, random_string):
        et = expr.and_(random_string)
        self._test_op(expr, et, random_string, query.Operator.AND)

    def test_or_(self, expr, random_string):
        et = expr.or_(random_string)
        self._test_op(expr, et, random_string, query.Operator.OR)

    def test_diff(self, expr, random_string):
        et = expr.diff(random_string)
        self._test_op(expr, et, random_string, query.Operator.DIFF)

    def test___invert__(self, expr):
        et = ~expr
        self._test_op(None, et, expr.value, query.Operator.INVERT)

    def test___and__(self, expr, random_string):
        et = (expr & random_string)
        self._test_op(expr, et, random_string, query.Operator.BIT_AND)

    def test___or__(self, expr, random_string):
        et = (expr | random_string)
        self._test_op(expr, et, random_string, query.Operator.BIT_OR)

    def test___xor__(self, expr, random_string):
        et = (expr ^ random_string)
        self._test_op(expr, et, random_string, query.Operator.BIT_XOR)

    def test___lshift__(self, expr, random_string):
        et = (expr << random_string)
        self._test_op(expr, et, random_string, query.Operator.LSHIFT)

    def test___rshift__(self, expr, random_string):
        et = (expr >> random_string)
        self._test_op(expr, et, random_string, query.Operator.RSHIFT)

    def test___iadd__(self, expr, random_string):
        et = expr
        et += random_string
        self._test_op(expr, et, random_string, query.Operator.IADD)

    def test___isub__(self, expr, random_string):
        et = expr
        et -= random_string
        self._test_op(expr, et, random_string, query.Operator.ISUB)

    def test___imul__(self, expr, random_string):
        et = expr
        et *= random_string
        self._test_op(expr, et, random_string, query.Operator.IMUL)

    def test___idiv__(self, expr, random_string):
        et = expr
        et /= random_string
        self._test_op(expr, et, random_string, query.Operator.IDIV)

    def test___imod__(self, expr, random_string):
        et = expr
        et %= random_string
        self._test_op(expr, et, random_string, query.Operator.IMOD)

    def test___ilshift__(self, expr, random_string):
        et = expr
        et <<= random_string
        self._test_op(expr, et, random_string, query.Operator.ILSHIFT)

    def test___irshift__(self, expr, random_string):
        et = expr
        et >>= random_string
        self._test_op(expr, et, random_string, query.Operator.IRSHIFT)

    def test___iand__(self, expr, random_string):
        et = expr
        et &= random_string
        self._test_op(expr, et, random_string, query.Operator.IBIT_AND)

    def test___ior__(self, expr, random_string):
        et = expr
        et |= random_string
        self._test_op(expr, et, random_string, query.Operator.IBIT_OR)

    def test___ixor__(self, expr, random_string):
        et = expr
        et ^= random_string
        self._test_op(expr, et, random_string, query.Operator.IBIT_XOR)


class TestExpression(BaseTestExpression):
    @pytest.fixture
    def Expression(self):
        return query.Expression

    @pytest.fixture
    def expr(self, Expression):
        return Expression('v1')

    def test___init__(self, Expression):
        expr = Expression('testvalue')
        assert (expr.value == 'testvalue') is True
        assert expr.lvalue is expr

    def test_wrap_expr_with_Expression_instances(self, Expression):
        expr1, expr2 = Expression('v1'), Expression('v2')
        exprs = tuple(Expression.wrap_expr(expr1, expr2))
        assert len(exprs) == 2
        assert exprs[0] is expr1
        assert exprs[1] is expr2

    def test_wrap_expr_with_ExpressionTree_instances(self, Expression):
        et1, et2 = (query.ExpressionTree('e1', 'l1', 'r1'),
                    query.ExpressionTree('e2', 'l2', 'r2'))
        exprs = tuple(Expression.wrap_expr(et1, et2))
        assert len(exprs) == 2
        assert exprs[0] is et1
        assert exprs[1] is et2

    def test_wrap_expr_with_None(self, Expression):
        exprs = tuple(Expression.wrap_expr(None))
        assert len(exprs) == 1
        assert exprs[0] is None

    def test_wrap_expr(self, Expression):
        exprs = tuple(Expression.wrap_expr('v1', 'v2', 10))
        assert len(exprs) == 3
        assert isinstance(exprs[0], query.Expression)
        assert isinstance(exprs[1], query.Expression)
        assert isinstance(exprs[2], query.Expression)
        assert (exprs[0].value == 'v1') is True
        assert (exprs[1].value == 'v2') is True
        assert (exprs[2].value == 10) is True

    def test_build_without_nested(self, Expression):
        expr = Expression('v1')
        result = expr.build(Expression)
        assert result == 'v1'

    def test_build_with_nested(self, Expression, random_string):
        class A(object):
            def __str__(self):
                return random_string
        expr = Expression(Expression(A()))
        result = expr.build(Expression)
        assert result == random_string

    @pytest.mark.parametrize(('value', 'expected'), (
        ('', ''),
        ('testvalue', 'testvalue'),
        ('foo bar', 'foo bar'),
        ('foo bar baz', 'foo bar baz'),
        (10, '10'),
        ("さくら咲き", "さくら咲き"),
        (u"さくら咲き", "さくら咲き"),
    ))
    def test___str__(self, Expression, value, expected):
        expr = Expression(value)
        assert str(expr) == expected


class TestMatchColumn(TestExpression):
    @pytest.fixture
    def Expression(self):
        return query.MatchColumn

    def test_constant(self):
        assert (query.MatchColumn.operator == {
            query.Operator.OR: ' || ',
            query.Operator.MUL: ' * ',
        }) is True


class TestQueryExpression(TestExpression):
    @pytest.fixture
    def Expression(self):
        return query.QueryExpression

    def test_constant(self):
        assert (query.QueryExpression.operator == {
            query.Operator.EQUAL: ':',
            query.Operator.GREATER_EQUAL: ':>=',
            query.Operator.GREATER_THAN: ':>',
            query.Operator.LESS_EQUAL: ':<=',
            query.Operator.LESS_THAN: ':<',
            query.Operator.NOT_EQUAL: ':!',
            query.Operator.BIT_OR: ' OR ',
            query.Operator.BIT_AND: ' + ',
            query.Operator.SUB: ' - ',
        }) is True

    @pytest.mark.parametrize(('value', 'expected'), (
        ('', ''),
        ('testvalue', 'testvalue'),
        ('foo bar', '"foo bar"'),
        ('foo bar baz', '"foo bar baz"'),
        (10, '10'),
        ("さくら咲き", "さくら咲き"),
        (u"さくら咲き", "さくら咲き"),
    ))
    def test___str__(self, Expression, value, expected):
        expr = Expression(value)
        assert str(expr) == expected


class TestFilterExpression(TestExpression):
    @pytest.fixture
    def Expression(self):
        return query.FilterExpression

    def test_constant(self):
        assert (query.FilterExpression.operator == {
            query.Operator.ADD: ' + ',
            query.Operator.SUB: ' - ',
            query.Operator.MUL: ' * ',
            query.Operator.DIV: ' / ',
            query.Operator.MOD: ' % ',
            query.Operator.NOT: '!',
            query.Operator.AND: ' && ',
            query.Operator.OR: ' || ',
            query.Operator.DIFF: ' &! ',
            query.Operator.INVERT: '~',
            query.Operator.BIT_AND: ' & ',
            query.Operator.BIT_OR: ' | ',
            query.Operator.BIT_XOR: ' ^ ',
            query.Operator.LSHIFT: ' << ',
            query.Operator.RSHIFT: ' >>> ',
            query.Operator.EQUAL: ' == ',
            query.Operator.NOT_EQUAL: ' != ',
            query.Operator.LESS_THAN: ' < ',
            query.Operator.LESS_EQUAL: ' <= ',
            query.Operator.GREATER_THAN: ' > ',
            query.Operator.GREATER_EQUAL: ' >= ',
            query.Operator.IADD: ' += ',
            query.Operator.ISUB: ' -= ',
            query.Operator.IMUL: ' *= ',
            query.Operator.IDIV: ' /= ',
            query.Operator.IMOD: ' %= ',
            query.Operator.ILSHIFT: ' <<= ',
            query.Operator.IRSHIFT: ' >>>= ',
            query.Operator.IBIT_AND: ' &= ',
            query.Operator.IBIT_OR: ' |= ',
            query.Operator.IBIT_XOR: ' ^= ',
            query.Operator.MATCH: ' @ ',
            query.Operator.STARTSWITH: ' @^ ',
            query.Operator.ENDSWITH: ' @$ ',
            query.Operator.NEAR: ' *N ',
            query.Operator.SIMILAR: ' *S ',
            query.Operator.TERM_EXTRACT: ' *T ',
            }) is True

    str_test_params = (
        ('', ''),
        ('testvalue', 'testvalue'),
        ('foo bar', '"foo bar"'),
        ('foo bar baz', '"foo bar baz"'),
        (10, '10'),
        ("さくら咲き", "さくら咲き"),
        (u"さくら咲き", "さくら咲き"),
        (0, '0'),
        (True, 'true'),
        (False, 'false'),
        (None, 'null'),
        (datetime(2013, 8, 20, 20, 19, 44, 128374),
         '"2013/08/20 20:19:44.128374"'),
        (date(2013, 9, 26), '"2013/09/26 00:00:00.000000"'),
        )

    @pytest.mark.parametrize(('value', 'expected'), str_test_params)
    def test___str__(self, Expression, value, expected):
        expr = Expression(value)
        assert str(expr) == expected


def test_GE():
    assert query.GE is query.Expression


class TestExpressionTree(BaseTestExpression):
    @pytest.fixture
    def Expression(self):
        return query.ExpressionTree

    @pytest.fixture
    def expr(self, Expression):
        return Expression('expr1', 'left', 'right')

    @pytest.fixture
    def Expr(self):
        class C(object):
            def __init__(self, value):
                self.value = value

            def __str__(self):
                return str(self.value)
        return C

    def _test_op_tree(self, left, result, expected, op):
        assert (result.op == op) is True
        assert result.left is left
        assert isinstance(result.right, query.ExpressionTree)
        assert result.right is expected

    def test___init__(self):
        et = query.ExpressionTree('testexpr', 'testleft', 'testright')
        assert (et.op == 'testexpr') is True
        assert isinstance(et.left, query.Expression)
        assert isinstance(et.right, query.Expression)
        assert (et.left.value, et.right.value) == ('testleft', 'testright')

    def test_not_(self, expr):
        et = expr.not_()
        self._test_op_tree(None, et, expr, query.Operator.NOT)

    def test___invert__(self, expr):
        et = ~expr
        self._test_op_tree(None, et, expr, query.Operator.INVERT)

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
