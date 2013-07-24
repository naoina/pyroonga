# -*- coding: utf-8 -*-

from pyroonga.odm.attributes import (Symbol, TableFlagsFlag)
from pyroonga.tests import unittest


class TestSymbol(unittest.TestCase):
    def test___init__(self):
        symbol = Symbol('testsym1')
        self.assertEqual(symbol.name, 'testsym1')

        symbol = Symbol('testsym2')
        self.assertEqual(symbol.name, 'testsym2')

    def test___str__(self):
        symbol = Symbol('testsym1')
        self.assertEqual(str(symbol), 'testsym1')

        symbol = Symbol('testsym2')
        self.assertEqual(str(symbol), 'testsym2')


class TestTableFlagsFlag(unittest.TestCase):
    def test___init__(self):
        self.assertRaises(TypeError, TableFlagsFlag, 'flag1')

        sym1 = Symbol('flag1')
        flags = TableFlagsFlag(sym1)
        self.assertListEqual(flags, [sym1])

        sym2 = Symbol('flag2')
        flags = TableFlagsFlag(sym2)
        self.assertListEqual(flags, [sym2])

        sym3, sym4 = (Symbol('flag3'), Symbol('flag4'))
        flags = TableFlagsFlag([sym3, sym4])
        self.assertListEqual(flags, [sym3, sym4])

        sym5, sym6 = (Symbol('flag5'), Symbol('flag6'))
        flags = TableFlagsFlag((sym5, sym6))
        self.assertSequenceEqual(flags, (sym5, sym6))

    def test___or__(self):
        sym1, sym2 = (Symbol('flag1'), Symbol('flag2'))
        flags1, flags2 = (TableFlagsFlag(sym1), TableFlagsFlag(sym2))
        result = flags1 | flags2
        self.assertListEqual(result, [sym1, sym2])

        sym4 = Symbol('flag4')
        flags3 = flags1 | flags2
        result = TableFlagsFlag(sym4) | flags3
        self.assertListEqual(result, [sym4, sym1, sym2])

        sym5 = Symbol('flag5')
        flags5 = TableFlagsFlag(sym5)
        self.assertRaises(TypeError, flags5.__or__, 'invalid')
        self.assertRaises(TypeError, flags5.__or__, 0)

    def test___and__(self):
        sym1, sym2 = (Symbol('flag1'), Symbol('flag2'))
        flags = TableFlagsFlag([sym1, sym2])
        flag1 = TableFlagsFlag(sym1)
        self.assertTrue(flags & flag1)

        flag2 = TableFlagsFlag(Symbol('flag3'))
        self.assertFalse(flags & flag2)

    def test___str__(self):
        sym1 = Symbol('flag1')
        flags1 = TableFlagsFlag(sym1)
        self.assertEqual(str(flags1), 'flag1')

        sym2 = Symbol('flag2')
        flags2 = TableFlagsFlag([sym1, sym2])
        self.assertEqual(str(flags2), 'flag1|flag2')

        sym3 = Symbol('flag3')
        flags3 = TableFlagsFlag([sym1, sym2, sym3])
        self.assertEqual(str(flags3), 'flag1|flag2|flag3')


def main():
    unittest.main()

if __name__ == '__main__':
    main()
