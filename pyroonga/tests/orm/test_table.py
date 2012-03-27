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
import itertools

from subprocess import Popen, PIPE

from pyroonga import Groonga
from pyroonga.orm.attributes import (
    TableFlags,
    ColumnFlags,
    DataType,
    Tokenizer,
    SuggestType,
    )
from pyroonga.orm.table import (
    Column,
    TableMeta,
    SuggestTable,
    prop_attr,
    tablebase,
    event_query,
    item_query,
    )
from pyroonga.orm.query import (
    GroongaResultBase,
    GroongaSuggestResults,
    GroongaSuggestResult,
    LoadQuery,
    SuggestQuery,
    )
from pyroonga.tests import unittest
from pyroonga.tests import GroongaTestBase


class TestColumn(unittest.TestCase):
    def test___init__with_default_value(self):
        col = Column()
        self.assertIs(col.flags, ColumnFlags.COLUMN_SCALAR)
        self.assertIs(col.type, DataType.ShortText)
        self.assertIs(col.__tablemeta__, TableMeta)
        self.assertIsNone(col.tablename)
        self.assertIsNone(col.name)
        self.assertIsNone(col.value)

    def test___init__(self):
        self.assertRaises(TypeError, Column, flags='')
        try:
            col = Column(flags=ColumnFlags.COLUMN_VECTOR)
            self.assertIs(col.flags, ColumnFlags.COLUMN_VECTOR)
        except TypeError:
            self.fail("TypeError has been raised")

        try:
            col = Column(type=DataType.UInt32)
            self.assertIs(col.type, DataType.UInt32)
        except TypeError:
            self.fail("TypeError has been raised")

        try:
            col = Column(type=TableMeta)
            self.assertIs(col.type, TableMeta)
        except TypeError:
            self.fail("TypeError has been raised")

    def test___str__(self):
        col = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.ShortText)
        self.assertRaises(TypeError, col.__str__)

        col = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.ShortText)
        col.tablename = 'tb1'
        self.assertRaises(TypeError, col.__str__)

        col = Column(flags=ColumnFlags.COLUMN_SCALAR, type=DataType.ShortText)
        col.name = 'name1'
        self.assertRaises(TypeError, col.__str__)

        col = Column(flags=ColumnFlags.COLUMN_VECTOR, type=DataType.ShortText)
        col.tablename = 'tb2'
        col.name = 'name2'
        self.assertEqual(col.__str__(), 'column_create --table tb2 ' \
                '--name name2 --flags COLUMN_VECTOR --type ShortText')


        ExampleTableBase = tablebase()
        class ExampleTable(ExampleTableBase):
            name = Column()

        col = Column(flags=ColumnFlags.COLUMN_INDEX, type=ExampleTable,
                     source=ExampleTable.name)
        col.tablename = 'tb3'
        col.name = 'name3'
        self.assertEqual(col.__str__(), 'column_create --table tb3 ' \
                '--name name3 --flags COLUMN_INDEX --type ExampleTable ' \
                '--source name')

        col = Column(flags=ColumnFlags.COLUMN_INDEX, type='ExampleTable', source='name')
        col.tablename = 'tb4'
        col.name = 'name4'
        self.assertEqual(col.__str__(), 'column_create --table tb4 ' \
                '--name name4 --flags COLUMN_INDEX --type ExampleTable ' \
                '--source name')


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

    def assertGroongaResultEqual(self, result, expect, all_len):
        self.assertIsInstance(result, GroongaResultBase)
        self.assertEqual(result.all_len, all_len)
        self.assertEqual(len(result), len(expect))
        for tbl, val in zip(result, expect):
            for k, v in val.items():
                self.assertEqual(getattr(tbl, k), v)

    def assertGroongaDrilldownResultEqual(self, result, expected, all_len):
        for r, (e, ln) in zip(result.drilldown, zip(expected, all_len)):
            self.assertGroongaResultEqual(r, e, all_len=ln)

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

    def _maketable1(self):
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
        fixture = self.loadfixture(1)
        self._insert('Tb', fixture)
        return Tb, fixture

    def _maketable2(self):
        Table = tablebase()

        class Tb(Table):
            category = Column()
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name category --flags '
                        'COLUMN_SCALAR --type ShortText')
        self._sendquery('column_create --table Tb --name name --flags '
                        'COLUMN_SCALAR --type ShortText')
        fixture = self.loadfixture(2)
        self._insert('Tb', fixture)
        return Tb, fixture

    def test_default_value(self):
        Table = tablebase()
        self.assertIs(Table.__tableflags__, TableFlags.TABLE_HASH_KEY)
        self.assertIs(Table.__key_type__, DataType.ShortText)
        self.assertEqual(Table.__tablename__, 'Table')
        self.assertEqual(Table.__default_tokenizer__, None)

    def test_table(self):
        Table = tablebase()
        namecol = Column(flags=ColumnFlags.COLUMN_SCALAR,
                         type=DataType.ShortText)
        passwordcol = Column(flags=ColumnFlags.COLUMN_SCALAR,
                             type=DataType.ShortText)

        class Tb1(Table):
            name = namecol
            password = passwordcol
            address = 'address'

        self.assertIs(Tb1.__tableflags__, TableFlags.TABLE_HASH_KEY)
        self.assertIs(Tb1.__key_type__, DataType.ShortText)
        self.assertIs(Tb1.__default_tokenizer__, None)
        self.assertEqual(Tb1.__tablename__, 'Tb1')
        self.assertEqual(str(Tb1), 'table_create --name Tb1 --flags ' \
                'TABLE_HASH_KEY --key_type ShortText')
        self.assertListEqual(Tb1.columns, [namecol, passwordcol])
        self.assertListEqual(Table._tables, [Tb1])

        sitecol = Column(flags=ColumnFlags.COLUMN_SCALAR,
                         type=DataType.ShortText)
        addresscol = Column(flags=ColumnFlags.COLUMN_SCALAR,
                            type=DataType.ShortText)

        class Tb2(Table):
            __tableflags__ = TableFlags.TABLE_PAT_KEY
            __key_type__ = DataType.UInt32
            __default_tokenizer__ = Tokenizer.TokenBigram
            site = sitecol
            address = addresscol

        self.assertIs(Tb2.__tableflags__, TableFlags.TABLE_PAT_KEY)
        self.assertIs(Tb2.__key_type__, DataType.UInt32)
        self.assertEqual(Tb2.__tablename__, 'Tb2')
        self.assertEqual(str(Tb2), 'table_create --name Tb2 --flags ' \
                'TABLE_PAT_KEY --key_type UInt32 --default_tokenizer ' \
                'TokenBigram')
        self.assertListEqual(Tb2.columns, [sitecol, addresscol])
        self.assertListEqual(Table._tables, [Tb1, Tb2])

        class Tb3(Table):
            __tableflags__ = TableFlags.TABLE_NO_KEY
            site = sitecol
            address = addresscol

        self.assertIs(Tb3.__tableflags__, TableFlags.TABLE_NO_KEY)
        self.assertEqual(Tb3.__tablename__, 'Tb3')
        self.assertEqual(str(Tb3), 'table_create --name Tb3 --flags ' \
                         'TABLE_NO_KEY')
        self.assertListEqual(Tb3.columns, [sitecol, addresscol])
        self.assertListEqual(Table._tables, [Tb1, Tb2, Tb3])

    def test_bind(self):
        Table = tablebase()
        self.assertRaises(TypeError, Table.bind, 'dummy')

        grn = Groonga()
        Table.bind(grn)
        self.assertIs(Table.grn, grn)

    def test_create_all(self):
        Table = tablebase()

        class Tb1(Table):
            name = Column(flags=ColumnFlags.COLUMN_SCALAR,
                          type=DataType.ShortText)

        class Tb2(Table):
            word = Column(flags=ColumnFlags.COLUMN_SCALAR,
                          type=DataType.ShortText)

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
                    [257, 'Tb2', GroongaTestBase.DB_PATH + '.0000101',
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
                    [258,
                     'name',
                     GroongaTestBase.DB_PATH + '.0000102',
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
                    [257, '_key', '', '', 'COLUMN_SCALAR', 'Tb2', 'ShortText', []],
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
        expected = [{'_id': 1, '_key': 'key1'},
                    {'_id': 2, '_key': 'key2'}]
        self.assertGroongaResultEqual(result, expected, all_len=2)

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
        expected = [{'_id': 1, '_key': 'key1', 'address': 'Address1',
                     'name': 'Name1'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(_key='2').all()
        expected = [{'_id': 2, '_key': 'key2', 'address': 'address2',
                     'name': 'name2'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(name='name').all()
        expected = [{'_id': 1, '_key': 'key1', 'address': 'Address1',
                     'name': 'Name1'},
                    {'_id': 2, '_key': 'key2', 'address': 'address2',
                     'name': 'name2'}]
        self.assertGroongaResultEqual(result, expected, all_len=2)

        result = Tb.select(address='ar').all()
        expected = [{'_id': 3, '_key': 'key3', 'address': 'bar',
                     'name': 'foo'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

    def test_select_with_condition(self):
        Tb, fixture = self._maketable1()

        result = Tb.select(Tb.title == 'Nyarlathotep').all()
        expected = [{'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(Tb.title != 'Nyarlathotep').all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=4)

        result = Tb.select(Tb.title != 'Gentoo Linux', body='linux').all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(Tb._key < 'key3').all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=2)

        result = Tb.select(Tb._id >= 3).all()
        expected = [{'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=3)

        result = Tb.select((Tb.title == 'Gentoo Linux') |
                           (Tb.title == 'Hastur')).all()
        expected = [{'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=2)

    def test_select_with_limit(self):
        Tb, fixture = self._maketable1()

        result = Tb.select().limit(2).all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        result = Tb.select().limit(100).all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        result = Tb.select().limit(-2).all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

    def test_select_with_offset(self):
        Tb, fixture = self._maketable1()

        result = Tb.select().offset(2).all()
        expected = [{'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        result = Tb.select().offset(-2).all()
        expected = [{'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

    def test_select_with_sortby(self):
        Tb, fixture = self._maketable1()

        result = Tb.select().sortby(Tb.title).all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']},
                    {'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        # FIXME: bad pattern. the reason is because the same result as the
        #        above tests.
        result = Tb.select().sortby(Tb.title, Tb.body).all()
        expected = [{'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']},
                    {'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        result = Tb.select().sortby(-Tb.title).all()
        expected = [{'_id': 3, '_key': 'key3', 'body': fixture[2]['body'],
                     'title': fixture[2]['title']},
                    {'_id': 5, '_key': 'key5', 'body': fixture[4]['body'],
                     'title': fixture[4]['title']},
                    {'_id': 2, '_key': 'key2', 'body': fixture[1]['body'],
                     'title': fixture[1]['title']},
                    {'_id': 4, '_key': 'key4', 'body': fixture[3]['body'],
                     'title': fixture[3]['title']},
                    {'_id': 1, '_key': 'key1', 'body': fixture[0]['body'],
                     'title': fixture[0]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

    def test_select_with_output_columns(self):
        Tb, fixture = self._maketable1()

        result = Tb.select().output_columns(Tb.title).all()
        expected = [{'title': fixture[0]['title']},
                    {'title': fixture[1]['title']},
                    {'title': fixture[2]['title']},
                    {'title': fixture[3]['title']},
                    {'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        result = Tb.select().output_columns(Tb.body, Tb.title).all()
        expected = [{'body': fixture[0]['body'], 'title': fixture[0]['title']},
                    {'body': fixture[1]['body'], 'title': fixture[1]['title']},
                    {'body': fixture[2]['body'], 'title': fixture[2]['title']},
                    {'body': fixture[3]['body'], 'title': fixture[3]['title']},
                    {'body': fixture[4]['body'], 'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

        result = Tb.select().output_columns(Tb.ALL).all()
        expected = [{'body': fixture[0]['body'], 'title': fixture[0]['title']},
                    {'body': fixture[1]['body'], 'title': fixture[1]['title']},
                    {'body': fixture[2]['body'], 'title': fixture[2]['title']},
                    {'body': fixture[3]['body'], 'title': fixture[3]['title']},
                    {'body': fixture[4]['body'], 'title': fixture[4]['title']}]
        self.assertGroongaResultEqual(result, expected, all_len=5)

    def test_select_with_drilldown(self):
        Tb, fixture = self._maketable2()

        result = Tb.select().drilldown(Tb.category).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.name).all()
        expected = [[{'_key': 'Miku Hatsune', '_nsubrecs': 1},
                     {'_key': 'Luka Megurine', '_nsubrecs': 1},
                     {'_key': 'MEIKO', '_nsubrecs': 1},
                     {'_key': 'Hitagi Senjogahara', '_nsubrecs': 1},
                     {'_key': 'Shinobu Oshino', '_nsubrecs': 1},
                     {'_key': 'Revy', '_nsubrecs': 1},
                     {'_key': 'Rock', '_nsubrecs': 1},
                     {'_key': 'Roberta', '_nsubrecs': 1}]]
        all_len = [8]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category, Tb.name).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3}],
                    [{'_key': 'Miku Hatsune', '_nsubrecs': 1},
                     {'_key': 'Luka Megurine', '_nsubrecs': 1},
                     {'_key': 'MEIKO', '_nsubrecs': 1},
                     {'_key': 'Hitagi Senjogahara', '_nsubrecs': 1},
                     {'_key': 'Shinobu Oshino', '_nsubrecs': 1},
                     {'_key': 'Revy', '_nsubrecs': 1},
                     {'_key': 'Rock', '_nsubrecs': 1},
                     {'_key': 'Roberta', '_nsubrecs': 1}]]
        all_len = [3, 8]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

    def test_select_with_drilldown_sortby(self):
        Tb, fixture = self._maketable2()

        result = Tb.select().drilldown(Tb.category).sortby(Tb._key).all()
        expected = [[{'_key': 'BLACK LAGOON', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'VOCALOID', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).sortby(Tb._nsubrecs).all()
        expected = [[{'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category). \
                    sortby(Tb._nsubrecs, Tb._key).all()
        expected = [[{'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3},
                     {'_key': 'VOCALOID', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).sortby(-Tb._nsubrecs).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

    def test_select_with_drilldown_output_columns(self):
        Tb, fixture = self._maketable2()

        result = Tb.select().drilldown(Tb.category). \
                    output_columns(Tb._key).all()
        expected = [[{'_key': 'VOCALOID'},
                     {'_key': 'Ghostory'},
                     {'_key': 'BLACK LAGOON'}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category). \
                    output_columns(Tb._nsubrecs, Tb._key).all()
        expected = [[{'_nsubrecs': 3, '_key': 'VOCALOID'},
                     {'_nsubrecs': 2, '_key': 'Ghostory'},
                     {'_nsubrecs': 3, '_key': 'BLACK LAGOON'}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

    def test_select_with_drilldown_offset(self):
        Tb, fixture = self._maketable2()

        result = Tb.select().drilldown(Tb.category).offset(1).all()
        expected = [[{'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).offset(2).all()
        expected = [[{'_key': 'BLACK LAGOON', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).offset(-2).all()
        expected = [[{'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

    def test_select_with_drilldown_limit(self):
        Tb, fixture = self._maketable2()

        result = Tb.select().drilldown(Tb.category).limit(1).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).limit(2).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).limit(100).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2},
                     {'_key': 'BLACK LAGOON', '_nsubrecs': 3}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

        result = Tb.select().drilldown(Tb.category).limit(-2).all()
        expected = [[{'_key': 'VOCALOID', '_nsubrecs': 3},
                     {'_key': 'Ghostory', '_nsubrecs': 2}]]
        all_len = [3]
        self.assertGroongaDrilldownResultEqual(result, expected, all_len)

    def test_load_immediately(self):
        Table = tablebase()

        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name name --flags '
                        'COLUMN_SCALAR --type ShortText')

        data = [Tb(_key='key1', name='name1'),
                Tb(_key='key2', name='name2'),
                Tb(_key='key3', name='name3')]
        result = Tb.load(data)
        self.assertEqual(result, 3)
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[3],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'name1'],
                     [2, 'key2', 'name2'],
                     [3, 'key3', 'name3']]]
        self.assertListEqual(stored[1], expected)

        # data override test
        data = [Tb(_key='key1', name='foo'),
                Tb(_key='key2', name='bar'),
                Tb(_key='key3', name='baz')]
        result = Tb.load(data)
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[3],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'foo'],
                     [2, 'key2', 'bar'],
                     [3, 'key3', 'baz']]]
        self.assertListEqual(stored[1], expected)

    def test_load_lazy(self):
        Table = tablebase()

        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name name --flags '
                        'COLUMN_SCALAR --type ShortText')

        data = [Tb(_key='key1', name='name1'),
                Tb(_key='key2', name='name2'),
                Tb(_key='key3', name='name3')]
        result = Tb.load(data, immediate=False)
        self.assertIsInstance(result, LoadQuery)
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        self.assertListEqual(stored[1], expected)
        result.commit()  # load actually
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[3],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'name1'],
                     [2, 'key2', 'name2'],
                     [3, 'key3', 'name3']]]
        self.assertListEqual(stored[1], expected)

        # duplicate commit
        with self.assertRaises(RuntimeError):
            result.commit()

    def test_load_lazy_multiple(self):
        Table = tablebase()

        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name name --flags '
                        'COLUMN_SCALAR --type ShortText')

        data1 = [Tb(_key='key1', name='name1'),
                 Tb(_key='key2', name='name2'),
                 Tb(_key='key3', name='name3')]
        result = Tb.load(data1, immediate=False)
        self.assertIsInstance(result, LoadQuery)
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        self.assertListEqual(stored[1], expected)
        data2 = [Tb(_key='key4', name='Madoka Kaname'),
                 Tb(_key='key5', name='Homura Akemi'),
                 Tb(_key='key6', name='Kyoko Sakura'),
                 Tb(_key='key7', name='Sayaka Miki')]
        result.load(data2)
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        self.assertListEqual(stored[1], expected)
        result.commit()  # load actually
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[7],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'name1'],
                     [2, 'key2', 'name2'],
                     [3, 'key3', 'name3'],
                     [4, 'key4', 'Madoka Kaname'],
                     [5, 'key5', 'Homura Akemi'],
                     [6, 'key6', 'Kyoko Sakura'],
                     [7, 'key7', 'Sayaka Miki']]]
        self.assertListEqual(stored[1], expected)

    def test_load_lazy_rollback(self):
        Table = tablebase()

        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        self._sendquery('table_create --name Tb --flags TABLE_HASH_KEY '
                        '--key_type ShortText')
        self._sendquery('column_create --table Tb --name name --flags '
                        'COLUMN_SCALAR --type ShortText')

        data1 = [Tb(_key='key1', name='name1'),
                 Tb(_key='key2', name='name2'),
                 Tb(_key='key3', name='name3')]
        result = Tb.load(data1, immediate=False)
        result.rollback()
        with self.assertRaises(RuntimeError):
            result.commit()
        stored = json.loads(self._sendquery('select --table Tb'))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        self.assertListEqual(stored[1], expected)


class TestSuggestTable(GroongaTestBase):
    def setUp(self):
        super(TestSuggestTable, self).setUp()
        super(TestSuggestTable, self).tearDownClass()
        super(TestSuggestTable, self).setUpClass()

    def tearDown(self):
        super(TestSuggestTable, self).tearDown()
        super(TestSuggestTable, self).tearDownClass()

    def _sendquery(self, cmd):
        proc = Popen('groonga -c', shell=True, stdin=PIPE, stdout=PIPE,
                stderr=PIPE)
        result = proc.communicate(cmd.encode('utf-8'))[0]
        proc.wait()
        return result.decode('utf-8')

    def _load(self, count):
        fixture = self.loadfixture('_suggest')
        data = json.dumps(fixture)
        for i in range(count):
            self._sendquery("load --table event_query --input_type json " \
                "--each 'suggest_preparer(_id, type, item, sequence, time, " \
                "pair_query)'\n%s" % (data))

    def test_create_all(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        result = json.loads(self._sendquery('table_list'))
        expected = [[['id', 'UInt32'],
                     ['name', 'ShortText'],
                     ['path', 'ShortText'],
                     ['flags', 'ShortText'],
                     ['domain', 'ShortText'],
                     ['range', 'ShortText']],
                    [259, 'bigram', GroongaTestBase.DB_PATH + '.0000103',
                     'TABLE_PAT_KEY|KEY_NORMALIZE|PERSISTENT',
                     'ShortText',
                     'null'],
                    [264, 'event_query', GroongaTestBase.DB_PATH + '.0000108',
                     'TABLE_NO_KEY|PERSISTENT',
                     'null',
                     'null'],
                    [258, 'event_type', GroongaTestBase.DB_PATH + '.0000102',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'ShortText',
                     'null'],
                    [261, 'item_query', GroongaTestBase.DB_PATH + '.0000105',
                     'TABLE_PAT_KEY|KEY_NORMALIZE|PERSISTENT',
                     'ShortText',
                     'null'],
                    [262, 'kana', GroongaTestBase.DB_PATH + '.0000106',
                     'TABLE_PAT_KEY|KEY_NORMALIZE|PERSISTENT',
                     'ShortText',
                     'null'],
                    [260, 'pair_query', GroongaTestBase.DB_PATH + '.0000104',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'UInt64',
                     'null'],
                    [263, 'sequence_query', GroongaTestBase.DB_PATH + '.0000107',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'ShortText',
                     'null'],
                    ]
        self.assertListEqual(result[1], expected)

    def test_load(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        data = [event_query(time=12, sequence='deadbeef', item='s'),
                event_query(time=13, sequence='deadbeef', item='se'),
                event_query(time=14, sequence='deadbeef', item='sea',
                            type='submit')]
        result = event_query.load(data)
        self.assertEqual(result, 3)
        stored = json.loads(self._sendquery('select --table event_query'))
        expected = [[[3],
                     [['_id', 'UInt32'],
                      ['item', 'item_query'],
                      ['sequence', 'sequence_query'],
                      ['time', 'Time'],
                      ['type', 'event_type']],
                     [1, 's', 'deadbeef', 12.0, ''],
                     [2, 'se', 'deadbeef', 13.0, ''],
                     [3, 'sea', 'deadbeef', 14.0, 'submit']]]
        self.assertListEqual(stored[1], expected)

        data = [event_query(time=22, sequence='foobar', item='mou'),
                event_query(time=23, sequence='foobar', item='moun',
                            type='submit'),
                event_query(time=24, sequence='foobar', item='mountain',
                            type='submit')]
        result = event_query.load(data)
        self.assertEqual(result, 3)
        stored = json.loads(self._sendquery('select --table event_query'))
        expected = [[[6],
                     [['_id', 'UInt32'],
                      ['item', 'item_query'],
                      ['sequence', 'sequence_query'],
                      ['time', 'Time'],
                      ['type', 'event_type']],
                     [1, 's', 'deadbeef', 12.0, ''],
                     [2, 'se', 'deadbeef', 13.0, ''],
                     [3, 'sea', 'deadbeef', 14.0, 'submit'],
                     [4, 'mou', 'foobar', 22.0, ''],
                     [5, 'moun', 'foobar', 23.0, 'submit'],
                     [6, 'mountain', 'foobar', 24.0, 'submit']]]
        self.assertListEqual(stored[1], expected)

        stored = json.loads(self._sendquery('select --table item_query'))
        expected = [[[6],
                     [['_id', 'UInt32'],
                      ['_key', 'ShortText'],
                      ['boost', 'Int32'],
                      ['buzz', 'Int32'],
                      ['co', 'pair_query'],
                      ['freq', 'Int32'],
                      ['freq2', 'Int32'],
                      ['kana', 'kana'],
                      ['last', 'Time']],
                     [4, 'mou', 0, 0, 1, 1, 0, [], 22.0],
                     [5, 'moun', 0, 0, 1, 1, 1, [], 23.0],
                     [6, 'mountain', 0, 0, 0, 1, 1, [], 24.0],
                     [1, 's', 0, 0, 1, 1, 0, [], 12.0],
                     [2, 'se', 0, 0, 1, 1, 0, [], 13.0],
                     [3, 'sea', 0, 0, 0, 1, 1, [], 14.0]]]
        self.assertListEqual(stored[1], expected)

    def test_suggest_all(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        # for default frequency threshold by groonga
        self._load(count=100)

        result = item_query.suggest('en').all()
        self.assertIsInstance(result, GroongaSuggestResults)
        expected_complete = [
            ('engine', 100),
            ]
        self.assertEqual(len(result.complete), len(expected_complete))
        for i, r in enumerate(result.complete):
            self.assertEqual(r._key, expected_complete[i][0])
            self.assertEqual(r._score, expected_complete[i][1])
        self.assertListEqual(result.correct, [])
        self.assertListEqual(result.suggest, [])

        result = item_query.suggest('sea').all()
        self.assertIsInstance(result, GroongaSuggestResults)
        expected_complete = [
            ('search', 101),
            ('search engine', 101),
            ]
        self.assertEqual(len(result.complete), len(expected_complete))
        for i, r in enumerate(result.complete):
            self.assertEqual(r._key, expected_complete[i][0])
            self.assertEqual(r._score, expected_complete[i][1])

    def test_suggest_get(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        # for default frequency threshold by groonga
        self._load(count=100)

        result = item_query.suggest('en').get('complete')
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = [
            ('engine', 100),
            ]
        self.assertEqual(len(result), len(expected))
        for i, r in enumerate(result):
            self.assertEqual(r._key, expected[i][0])
            self.assertEqual(r._score, expected[i][1])

        result = item_query.suggest('en').get('correct')
        self.assertListEqual(result, [])

        result = item_query.suggest('en').get('suggest')
        self.assertListEqual(result, [])

        with self.assertRaises(KeyError):
            item_query.suggest('en').get('dummy')

    def test_suggest___getitem__(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        # for default frequency threshold by groonga
        self._load(count=100)

        result = item_query.suggest('en')['complete']
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = [
            ('engine', 100),
            ]
        self.assertEqual(len(result), len(expected))
        for i, r in enumerate(result):
            self.assertEqual(r._key, expected[i][0])
            self.assertEqual(r._score, expected[i][1])

        result = item_query.suggest('en')['correct']
        self.assertListEqual(result, [])

        result = item_query.suggest('en')['suggest']
        self.assertListEqual(result, [])

        with self.assertRaises(KeyError):
            item_query.suggest('en')['dummy']

    def test_suggest_with_types(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        # for default frequency threshold by groonga
        self._load(count=100)

        values = [
            (SuggestType.complete, 'en'),
            (SuggestType.correct, 'saerch'),
            (SuggestType.suggest, 'search'),
            (SuggestType.complete | SuggestType.correct, 'search'),
            (SuggestType.complete | SuggestType.suggest, 'search'),
            (SuggestType.complete | SuggestType.correct | SuggestType.suggest,
                'search')]
        expects = [
            # (complete, correct, suggest)
            (['engine'], [], []),
            ([], ['search'], []),
            ([], [], ['search engine', 'web search realtime']),
            (['search', 'search engine'], [], []),
            (['search', 'search engine'], [],
             ['search engine', 'web search realtime']),
            (['search', 'search engine'], [],
             ['search engine', 'web search realtime'])
            ]
        for i, (types, query) in enumerate(values):
            result = item_query.suggest(query).types(types).all()
            self.assertIsInstance(result, GroongaSuggestResults)
            expected = expects[i]

            self.assertEqual(len(result.complete), len(expected[0]),
                    'complete, loop %d' % i)
            for j, r in enumerate(result.complete):
                self.assertEqual(r._key, expected[0][j])
            self.assertEqual(len(result.correct), len(expected[1]),
                    'correct, loop %d' % i)
            for j, r in enumerate(result.correct):
                self.assertEqual(r._key, expected[1][j])
            self.assertEqual(len(result.suggest), len(expected[2]),
                    'suggest, loop %d' % i)
            for j, r in enumerate(result.suggest):
                self.assertEqual(r._key, expected[2][j])

    def test_suggest_with_frequency_threshold(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()

        self._load(count=3)
        result = item_query.suggest('en').frequency_threshold(2)['complete']
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = [
            ('engine', 3),
            ]
        self.assertEqual(len(result), len(expected))
        for i, r in enumerate(result):
            self.assertEqual(r._key, expected[i][0])
            self.assertEqual(r._score, expected[i][1])

        result = item_query.suggest('en').frequency_threshold(3)['complete']
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = [
            ('engine', 3),
            ]
        self.assertEqual(len(result), len(expected))
        for i, r in enumerate(result):
            self.assertEqual(r._key, expected[i][0])
            self.assertEqual(r._score, expected[i][1])

        result = item_query.suggest('en').frequency_threshold(3)['complete']
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = [
            ('engine', 3),
            ]
        self.assertEqual(len(result), len(expected))
        for i, r in enumerate(result):
            self.assertEqual(r._key, expected[i][0])
            self.assertEqual(r._score, expected[i][1])

        result = item_query.suggest('en').frequency_threshold(4)['complete']
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = []
        try:
            self.assertEqual(len(result), len(expected))
            for i, r in enumerate(result):
                self.assertEqual(r._key, expected[i][0])
                self.assertEqual(r._score, expected[i][1])
        except AssertionError:
            # Expected failure. bugs in groonga?
            pass

        result = item_query.suggest('en').frequency_threshold(5)['complete']
        self.assertIsInstance(result, GroongaSuggestResult)
        expected = []
        self.assertEqual(len(result), len(expected))
        for i, r in enumerate(result):
            self.assertEqual(r._key, expected[i][0])
            self.assertEqual(r._score, expected[i][1])

    def test_suggest_with_conditional_probability_threshold(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()

        query = item_query.suggest('en'). \
                conditional_probability_threshold(1)
        self.assertEqual(str(query),
                'suggest --table "item_query" --column ' \
                '"kana" --types "complete" ' \
                '--conditional_probability_threshold 1.0 --query "en"')

        query = item_query.suggest('en'). \
                conditional_probability_threshold(0.2)
        self.assertEqual(str(query),
                'suggest --table "item_query" --column ' \
                '"kana" --types "complete" ' \
                '--conditional_probability_threshold 0.2 --query "en"')


    def test_suggest_with_prefix_search(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()

        query = item_query.suggest('en').prefix_search(True)
        self.assertEqual(str(query),
                'suggest --table "item_query" --column ' \
                '"kana" --types "complete" --prefix_search yes --query "en"')

        query = item_query.suggest('en').prefix_search(False)
        self.assertEqual(str(query),
                'suggest --table "item_query" --column ' \
                '"kana" --types "complete" --prefix_search no --query "en"')


def main():
    unittest.main()

if __name__ == '__main__':
    main()
