# -*- coding: utf-8 -*-

import pytest

from pyroonga.odm.attributes import Symbol, TableFlagsFlag


class TestSymbol(object):
    @pytest.mark.parametrize('name', (
        'testsym1',
        'testsym2',
        'testsym3',
    ))
    def test___init__(self, name):
        symbol = Symbol(name)
        assert symbol.name == name

    @pytest.mark.parametrize('name', (
        'testsym1',
        'testsym2',
        'testsym3',
    ))
    def test___str__(self, name):
        symbol = Symbol(name)
        assert str(symbol) == name


class TestTableFlagsFlag(object):
    def test___init__with_invalid_value(self):
        with pytest.raises(TypeError):
            TableFlagsFlag('flag1')

    @pytest.mark.parametrize('name', (
        'flag1',
        'flag2',
        'flag3',
    ))
    def test___init__with_single_parameter(self, name):
        sym = Symbol(name)
        flags = TableFlagsFlag(sym)
        assert flags == [sym]

    @pytest.mark.parametrize(('name1', 'name2'), (
        ('flag3', 'flag4'),
        ('flag5', 'flag6'),
    ))
    def test___init__with_list(self, name1, name2):
        sym1, sym2 = (Symbol(name1), Symbol(name2))
        flags = TableFlagsFlag([sym1, sym2])
        assert flags == [sym1, sym2]

    @pytest.mark.parametrize(('name1', 'name2'), (
        ('flag7', 'flag8'),
        ('flag9', 'flag10'),
    ))
    def test___init__with_tuple(self, name1, name2):
        sym1, sym2 = (Symbol(name1), Symbol(name2))
        flags = TableFlagsFlag((sym1, sym2))
        assert flags == [sym1, sym2]

    def test___or__(self):
        sym1, sym2 = (Symbol('flag1'), Symbol('flag2'))
        flags1, flags2 = (TableFlagsFlag(sym1), TableFlagsFlag(sym2))
        result = flags1 | flags2
        assert result == [sym1, sym2]

        sym4 = Symbol('flag4')
        flags3 = flags1 | flags2
        result = TableFlagsFlag(sym4) | flags3
        assert result == [sym4, sym1, sym2]

    @pytest.mark.parametrize('value', (
        'invalid',
        0,
    ))
    def test___or__with_invalid_param(self, value):
        sym = Symbol('flag5')
        flags = TableFlagsFlag(sym)
        with pytest.raises(TypeError):
            flags.__or__(value)

    def test___and__(self):
        sym1, sym2 = (Symbol('flag1'), Symbol('flag2'))
        flags = TableFlagsFlag([sym1, sym2])
        flag1 = TableFlagsFlag(sym1)
        assert (flags & flag1) is True

        flag2 = TableFlagsFlag(Symbol('flag3'))
        assert (flags & flag2) is False

    @pytest.mark.parametrize(('params', 'expected'), (
        (Symbol('flag1'), 'flag1'),
        ([Symbol('flag1'), Symbol('flag2')], 'flag1|flag2'),
        ([Symbol('flag1'), Symbol('flag2'), Symbol('flag3')],
         'flag1|flag2|flag3'),
    ))
    def test___str__(self, params, expected):
        flags = TableFlagsFlag(params)
        assert str(flags) == expected
