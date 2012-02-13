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

import json

from subprocess import Popen, PIPE

from pyroonga import Groonga
from pyroonga.orm.attributes import (COLUMN_SCALAR, COLUMN_VECTOR,
                                     TABLE_HASH_KEY, TABLE_PAT_KEY, ShortText,
                                     UInt32)
from pyroonga.orm.table import Column, prop_attr, tablebase
from pyroonga.tests import unittest
from pyroonga.tests import GroongaTestBase


class TestColumn(unittest.TestCase):
    def test___init__with_default_value(self):
        col = Column()
        self.assertIs(col.flags, COLUMN_SCALAR)
        self.assertIs(col.type, ShortText)
        self.assertIsNone(col.tablename)
        self.assertIsNone(col.name)
        self.assertIsNone(col.value)

    def test___init__(self):
        self.assertRaises(TypeError, Column, flags='')
        self.assertRaises(TypeError, Column, type='')
        try:
            col = Column(flags=COLUMN_VECTOR)
            self.assertIs(col.flags, COLUMN_VECTOR)
        except TypeError:
            self.fail("TypeError has been raised")

        try:
            col = Column(type=UInt32)
            self.assertIs(col.type, UInt32)
        except TypeError:
            self.fail("TypeError has been raised")

    def test___str__(self):
        col = Column(flags=COLUMN_SCALAR, type=ShortText)
        self.assertRaises(TypeError, col.__str__)

        col = Column(flags=COLUMN_SCALAR, type=ShortText)
        col.tablename = 'tb1'
        self.assertRaises(TypeError, col.__str__)

        col = Column(flags=COLUMN_SCALAR, type=ShortText)
        col.name = 'name1'
        self.assertRaises(TypeError, col.__str__)

        col = Column(flags=COLUMN_VECTOR, type=ShortText)
        col.tablename = 'tb2'
        col.name = 'name2'
        self.assertEqual(col.__str__(), 'column_create --table tb2 ' \
                '--name name2 --flags COLUMN_VECTOR --type ShortText')


class TestPropAttr(unittest.TestCase):
    def test_prop_attr(self):
        class TestClass(object):
            name = 'dummy'

            @prop_attr
            def __tablename__(cls):
                return cls.name

        self.assertEqual(TestClass.__tablename__, 'dummy')
        TestClass.name = 'tbname1'
        self.assertEqual(TestClass.__tablename__, 'tbname1')


class TestTable(GroongaTestBase):
    def setUp(self):
        super(TestTable, self).setUp()
        super(TestTable, self).tearDownClass()
        super(TestTable, self).setUpClass()

    def tearDown(self):
        super(TestTable, self).tearDown()
        super(TestTable, self).tearDownClass()

    def _sendquery(self, cmd):
        proc = Popen('groonga -c', shell=True, stdin=PIPE, stdout=PIPE,
                stderr=PIPE)
        result = proc.communicate(cmd.encode('utf-8'))[0]
        proc.wait()
        return result.decode('utf-8')

    def _insert(self, tbl, data):
        data = json.dumps(data)
        self._sendquery('load --table %s --input_type json --values\n%s' %
                (tbl, data))

    def test_default_value(self):
        Table = tablebase()
        self.assertIs(Table.__tableflags__, TABLE_HASH_KEY)
        self.assertIs(Table.__key_type__, ShortText)
        self.assertEqual(Table.__tablename__, 'Table')

    def test_table(self):
        Table = tablebase()
        namecol = Column(flags=COLUMN_SCALAR, type=ShortText)
        passwordcol = Column(flags=COLUMN_SCALAR, type=ShortText)

        class Tb1(Table):
            name = namecol
            password = passwordcol
            address = 'address'

        self.assertIs(Tb1.__tableflags__, TABLE_HASH_KEY)
        self.assertIs(Tb1.__key_type__, ShortText)
        self.assertEqual(Tb1.__tablename__, 'Tb1')
        self.assertEqual(str(Tb1), 'table_create --name Tb1 --flags ' \
                'TABLE_HASH_KEY --key_type ShortText')
        self.assertListEqual(Tb1.columns, [namecol, passwordcol])
        self.assertListEqual(Table._tables, [Tb1])

        sitecol = Column(flags=COLUMN_SCALAR, type=ShortText)
        addresscol = Column(flags=COLUMN_SCALAR, type=ShortText)

        class Tb2(Table):
            __tableflags__ = TABLE_PAT_KEY
            __key_type__ = UInt32
            site = sitecol
            address = addresscol

        self.assertIs(Tb2.__tableflags__, TABLE_PAT_KEY)
        self.assertIs(Tb2.__key_type__, UInt32)
        self.assertEqual(Tb2.__tablename__, 'Tb2')
        self.assertEqual(str(Tb2), 'table_create --name Tb2 --flags ' \
                'TABLE_PAT_KEY --key_type UInt32')
        self.assertListEqual(Tb2.columns, [sitecol, addresscol])
        self.assertListEqual(Table._tables, [Tb1, Tb2])

    def test_bind(self):
        Table = tablebase()
        self.assertRaises(TypeError, Table.bind, 'dummy')

        grn = Groonga()
        Table.bind(grn)
        self.assertIs(Table.grn, grn)

    def test_create_all(self):
        Table = tablebase()

        class Tb1(Table):
            name = Column(flags=COLUMN_SCALAR, type=ShortText)

        class Tb2(Table):
            word = Column(flags=COLUMN_SCALAR, type=ShortText)

        grn = Groonga()
        Table.bind(grn)
        Table.create_all()
        result = json.loads(self._sendquery('table_list'))
        expected = [[['id', 'UInt32'],
                     ['name', 'ShortText'],
                     ['path', 'ShortText'],
                     ['flags', 'ShortText'],
                     ['domain', 'ShortText'],
                     ['range', 'ShortText']],
                    [256, 'Tb1', GroongaTestBase.DB_PATH + '.0000100',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'ShortText',
                     'null'],
                    [258, 'Tb2', GroongaTestBase.DB_PATH + '.0000102',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'ShortText',
                     'null']]
        self.assertListEqual(result[1], expected)

        result = json.loads(self._sendquery('column_list Tb1'))
        expected = [[['id', 'UInt32'],
                     ['name', 'ShortText'],
                     ['path', 'ShortText'],
                     ['type', 'ShortText'],
                     ['flags', 'ShortText'],
                     ['domain', 'ShortText'],
                     ['range', 'ShortText'],
                     ['source', 'ShortText']],
                    [256, '_key', '', '', 'COLUMN_SCALAR', 'Tb1', 'ShortText', []],
                    [257,
                     'name',
                     GroongaTestBase.DB_PATH + '.0000101',
                     'var',
                     'COLUMN_SCALAR|PERSISTENT',
                     'Tb1',
                     'ShortText',
                     []]]
        self.assertListEqual(result[1], expected)

        result = json.loads(self._sendquery('column_list Tb2'))
        expected = [[['id', 'UInt32'],
                     ['name', 'ShortText'],
                     ['path', 'ShortText'],
                     ['type', 'ShortText'],
                     ['flags', 'ShortText'],
                     ['domain', 'ShortText'],
                     ['range', 'ShortText'],
                     ['source', 'ShortText']],
                    [258, '_key', '', '', 'COLUMN_SCALAR', 'Tb2', 'ShortText', []],
                    [259,
                     'word',
                     GroongaTestBase.DB_PATH + '.0000103',
                     'var',
                     'COLUMN_SCALAR|PERSISTENT',
                     'Tb2',
                     'ShortText',
                     []]]
        self.assertListEqual(result[1], expected)

    def test_select_all(self):
        Table = tablebase()

        class Tb(Table):
            pass

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._insert('Tb', [{'_key': 'key1'}, {'_key': 'key2'}])
        result = Tb.select().all()
        expected = [
            [[2], [['_id', 'UInt32'], ['_key', 'ShortText']],
             [1, 'key1'],
             [2, 'key2']]]
        self.assertListEqual(result, expected)

    def test_select_with_column(self):
        Table = tablebase()

        class Tb(Table):
            name = Column()
            address = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name name --flags '
                        'COLUMN_SCALAR --type ShortText')
        self._sendquery('column_create --table Tb --name address --flags '
                        'COLUMN_SCALAR --type ShortText')
        self._insert('Tb', [{'_key': 'key1', 'name': 'Name1',
                             'address': 'Address1'},
                            {'_key': 'key2', 'name': 'name2',
                             'address': 'address2'},
                            {'_key': 'key3', 'name': 'foo',
                             'address': 'bar'}])

        result = Tb.select(_key='1').all()
        expected = [[[1],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['address', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'Address1', 'Name1']]]
        self.assertListEqual(result, expected)

        result = Tb.select(_key='2').all()
        expected = [[[1],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['address', 'ShortText'],
                      ['name', 'ShortText']],
                     [2, 'key2', 'address2', 'name2']]]
        self.assertListEqual(result, expected)

        result = Tb.select(name='name').all()
        expected = [[[2],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['address', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'Address1', 'Name1'],
                     [2, 'key2', 'address2', 'name2']]]
        self.assertListEqual(result, expected)

        result = Tb.select(address='ar').all()
        expected = [[[1],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['address', 'ShortText'],
                      ['name', 'ShortText']],
                     [3, 'key3', 'bar', 'foo']]]
        self.assertListEqual(result, expected)

    def test_select_with_condition(self):
        Table = tablebase()

        class Tb(Table):
            title = Column()
            body = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name title --flags '
                        'COLUMN_SCALAR --type ShortText')
        self._sendquery('column_create --table Tb --name body --flags '
                        'COLUMN_SCALAR --type Text')
        fixture = self.loadfixture()
        self._insert('Tb', fixture)

        result = Tb.select(Tb.title == 'Nyarlathotep').all()
        expected = [[[1],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [3, 'key3', fixture[2]['body'], fixture[2]['title']]]]
        self.assertListEqual(result, expected)

        result = Tb.select(Tb.title != 'Nyarlathotep').all()
        expected = [[[4],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [1, 'key1', fixture[0]['body'], fixture[0]['title']],
                     [2, 'key2', fixture[1]['body'], fixture[1]['title']],
                     [4, 'key4', fixture[3]['body'], fixture[3]['title']],
                     [5, 'key5', fixture[4]['body'], fixture[4]['title']]]]
        self.assertListEqual(result, expected)

        result = Tb.select(Tb.title != 'Gentoo_Linux', body='linux').all()
        expected = [[[1],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [1, 'key1', fixture[0]['body'], fixture[0]['title']]]]
        self.assertListEqual(result, expected)

        result = Tb.select((Tb.title == 'Gentoo_Linux') |
                           (Tb.title == 'Hastur')).all()
        expected = [[[2],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [2, 'key2', fixture[1]['body'], fixture[1]['title']],
                     [5, 'key5', fixture[4]['body'], fixture[4]['title']]]]
        self.assertListEqual(result, expected)

    def test_select_with_limit(self):
        Table = tablebase()

        class Tb(Table):
            title = Column()
            body = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name title --flags '
                        'COLUMN_SCALAR --type ShortText')
        self._sendquery('column_create --table Tb --name body --flags '
                        'COLUMN_SCALAR --type Text')
        fixture = self.loadfixture()
        self._insert('Tb', fixture)

        result = Tb.select().limit(2).all()
        expected = [[[5],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [1, 'key1', fixture[0]['body'], fixture[0]['title']],
                     [2, 'key2', fixture[1]['body'], fixture[1]['title']]]]
        self.assertListEqual(result, expected)

        result = Tb.select().limit(100).all()
        expected = [[[5],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [1, 'key1', fixture[0]['body'], fixture[0]['title']],
                     [2, 'key2', fixture[1]['body'], fixture[1]['title']],
                     [3, 'key3', fixture[2]['body'], fixture[2]['title']],
                     [4, 'key4', fixture[3]['body'], fixture[3]['title']],
                     [5, 'key5', fixture[4]['body'], fixture[4]['title']]]]
        self.assertListEqual(result, expected)

        result = Tb.select().limit(-2).all()
        expected = [[[5],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['body', 'Text'],
                      ['title', 'ShortText']],
                     [1, 'key1', fixture[0]['body'], fixture[0]['title']],
                     [2, 'key2', fixture[1]['body'], fixture[1]['title']],
                     [3, 'key3', fixture[2]['body'], fixture[2]['title']],
                     [4, 'key4', fixture[3]['body'], fixture[3]['title']]]]
        self.assertListEqual(result, expected)


def main():
    unittest.main()

if __name__ == '__main__':
    main()
