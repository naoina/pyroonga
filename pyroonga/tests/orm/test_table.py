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
from pyroonga.orm.query import GroongaResultBase
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


def main():
    unittest.main()

if __name__ == '__main__':
    main()
