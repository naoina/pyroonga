# -*- coding: utf-8 -*-

import pytest

from pyroonga.odm import attributes as a, query, table

from pyroonga.tests.unit.odm import test_query


class TestColumn(test_query.BaseTestExpression):
    @pytest.fixture
    def Expression(self):
        return table.Column

    @pytest.fixture
    def expr(self, Expression):
        col = Expression()
        col.name = 'test_name'
        return col

    def _test_op(self, left, result, expected, op):
        assert (result.op == op) is True
        assert (result.left.value == left.name) is True
        assert isinstance(result.right, query.Expression)
        assert (result.right.value == expected) is True

    def test___init___with_default_value(self):
        col = table.Column()
        assert col.flags is a.ColumnFlags.COLUMN_SCALAR
        assert col.type is a.DataType.ShortText
        assert col.__tablemeta__ is table.TableMeta
        assert col.tablename is None
        assert col.name is None
        assert col.value is None

    def test___init___with_invalid_flags(self):
        with pytest.raises(TypeError):
            table.Column(flags='')

    def test___init___with_flags(self):
        try:
            col = table.Column(flags=a.ColumnFlags.COLUMN_VECTOR)
            assert col.flags is a.ColumnFlags.COLUMN_VECTOR
        except TypeError:
            assert False, "TypeError should not be raised"

    @pytest.mark.parametrize('type_', (
        a.DataType.UInt32,
        table.TableMeta,
    ))
    def test___init___with_type(self, type_):
        try:
            col = table.Column(type=type_)
            assert col.type is type_
        except TypeError:
            assert False, "TypeError should not be raised"

    def test_lvalue(self, random_string):
        col = table.Column()
        col.name = random_string
        assert (col.lvalue == random_string) is True

    def _test_op_column(self, left, result, expected, op):
        assert (result.op == op) is True
        assert result.left is left
        assert isinstance(result.right, query.Expression)
        assert (result.right.value == expected) is True

    def test_not_(self, expr):
        et = expr.not_()
        self._test_op_column(None, et, expr.name, query.Operator.NOT)

    def test___invert__(self, expr):
        et = ~expr
        self._test_op_column(None, et, expr.name, query.Operator.INVERT)

    def test___str___without_attributes(self):
        col = table.Column(flags=a.ColumnFlags.COLUMN_SCALAR, type=a.DataType.ShortText)
        with pytest.raises(TypeError):
            str(col)

    def test___str___without_name(self):
        col = table.Column(flags=a.ColumnFlags.COLUMN_SCALAR, type=a.DataType.ShortText)
        col.tablename = 'tb1'
        with pytest.raises(TypeError):
            str(col)

    def test___str___without_tablename(self):
        col = table.Column(flags=a.ColumnFlags.COLUMN_SCALAR, type=a.DataType.ShortText)
        col.name = 'name1'
        with pytest.raises(TypeError):
            str(col)

    def test___str___without_table(self):
        col = table.Column(flags=a.ColumnFlags.COLUMN_VECTOR, type=a.DataType.ShortText)
        col.tablename = 'tb2'
        col.name = 'name2'
        assert str(col) == ('column_create --table tb2 --name name2 --flags'
                            ' COLUMN_VECTOR --type ShortText')

    def test___str___with_class(self):
        ExampleTableBase = table.tablebase()

        class ExampleTable(ExampleTableBase):
            name = table.Column()

        col = table.Column(flags=a.ColumnFlags.COLUMN_INDEX, type=ExampleTable,
                     source=ExampleTable.name)
        col.tablename = 'tb3'
        col.name = 'name3'
        assert str(col) == ('column_create --table tb3 --name name3 --flags'
                            ' COLUMN_INDEX --type ExampleTable --source name')

    def test___str___with_class_string(self):
        ExampleTableBase = table.tablebase()

        class ExampleTable(ExampleTableBase):
            name = table.Column()

        col = table.Column(flags=a.ColumnFlags.COLUMN_INDEX, type='ExampleTable',
                     source='name')
        col.tablename = 'tb4'
        col.name = 'name4'
        assert str(col) == ('column_create --table tb4 --name name4 --flags'
                            ' COLUMN_INDEX --type ExampleTable --source name')
