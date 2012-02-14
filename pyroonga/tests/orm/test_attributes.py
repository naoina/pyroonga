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

from pyroonga.orm.attributes import (Symbol, TableFlags)
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


class TestTableFlags(unittest.TestCase):
    def test___init__(self):
        self.assertRaises(TypeError, TableFlags, 'flag1')

        sym1 = Symbol('flag1')
        flags = TableFlags(sym1)
        self.assertListEqual(flags, [sym1])

        sym2 = Symbol('flag2')
        flags = TableFlags(sym2)
        self.assertListEqual(flags, [sym2])

        sym3, sym4 = (Symbol('flag3'), Symbol('flag4'))
        flags = TableFlags([sym3, sym4])
        self.assertListEqual(flags, [sym3, sym4])

        sym5, sym6 = (Symbol('flag5'), Symbol('flag6'))
        flags = TableFlags((sym5, sym6))
        self.assertSequenceEqual(flags, (sym5, sym6))

    def test___or__(self):
        sym1, sym2 = (Symbol('flag1'), Symbol('flag2'))
        flags1, flags2 = (TableFlags(sym1), TableFlags(sym2))
        result = flags1 | flags2
        self.assertListEqual(result, [sym1, sym2])

        sym4 = Symbol('flag4')
        flags3 = flags1 | flags2
        result = TableFlags(sym4) | flags3
        self.assertListEqual(result, [sym4, sym1, sym2])

        sym5 = Symbol('flag5')
        flags5 = TableFlags(sym5)
        self.assertRaises(TypeError, flags5.__or__, 'invalid')
        self.assertRaises(TypeError, flags5.__or__, 0)

    def test___contains__(self):
        sym1, sym2 = (Symbol('flag1'), Symbol('flag2'))
        flags = TableFlags([sym1, sym2])
        flag1 = TableFlags(sym1)
        self.assertTrue(flags & flag1)

        flag2 = TableFlags(Symbol('flag3'))
        self.assertFalse(flags & flag2)

    def test___str__(self):
        sym1 = Symbol('flag1')
        flags1 = TableFlags(sym1)
        self.assertEqual(str(flags1), 'flag1')

        sym2 = Symbol('flag2')
        flags2 = TableFlags([sym1, sym2])
        self.assertEqual(str(flags2), 'flag1|flag2')

        sym3 = Symbol('flag3')
        flags3 = TableFlags([sym1, sym2, sym3])
        self.assertEqual(str(flags3), 'flag1|flag2|flag3')


def main():
    unittest.main()

if __name__ == '__main__':
    main()
