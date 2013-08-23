# -*- coding: utf-8 -*-

import json
import random

import pytest

from pyroonga import Groonga, exceptions
from pyroonga.odm import attributes, query
from pyroonga.odm.attributes import (
    TableFlags,
    ColumnFlags,
    DataType,
    Normalizer,
    Tokenizer,
    SuggestType,
    )
from pyroonga.odm.table import (
    Column,
    SuggestTable,
    prop_attr,
    tablebase,
    event_query,
    item_query,
    )
from pyroonga.odm.query import (
    GroongaResultBase,
    GroongaSuggestResults,
    GroongaSuggestResult,
    LoadQuery,
    )
from pyroonga import utils
from pyroonga.tests import utils as test_utils


class TestPropAttr(object):
    def test_prop_attr(self):
        class TestClass(object):
            name = 'dummy'

            @prop_attr
            def __tablename__(cls):
                return cls.name

        assert (TestClass.__tablename__ == 'dummy') is True
        TestClass.name = 'tbname1'
        assert (TestClass.__tablename__ == 'tbname1') is True


class TestTable(object):
    def get_tableinfo(self, *args):
        result = utils.to_python(
            json.loads(test_utils.sendquery('table_list'))[1], 0)
        tableinfo = []
        for name in args:
            for t in result:
                if name == t['name']:
                    tableinfo.append(t)
        return tableinfo

    def get_columninfo(self, tablename):
        result = json.loads(test_utils.sendquery('column_list %s' % tablename))
        return utils.to_python(result[1], 0)

    def assertGroongaResultEqual(self, result, expect, all_len):
        assert isinstance(result, GroongaResultBase)
        assert result.all_len == all_len
        assert len(result) == len(expect)
        for tbl, val in zip(result, expect):
            for k, v in val.items():
                assert (getattr(tbl, k) == v) is True

    def assertGroongaDrilldownResultEqual(self, result, expected, all_len):
        for r, (e, ln) in zip(result.drilldown, zip(expected, all_len)):
            self.assertGroongaResultEqual(r, e, all_len=ln)

    def _insert(self, tbl, data):
        data = json.dumps(data)
        test_utils.sendquery('load --table %s --input_type json --values\n%s' %
                (tbl, data))

    @pytest.fixture
    def Table1(self, Table, fixture1):
        class Tb(Table):
            title = Column()
            body = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name title --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name body --flags'
                             ' COLUMN_SCALAR --type Text' % Tb.__tablename__)
        self._insert(Tb.__tablename__, fixture1)
        return Tb

    @pytest.fixture
    def Table2(self, Table, fixture2):
        class Tb(Table):
            category = Column()
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name category --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        self._insert(Tb.__tablename__, fixture2)
        return Tb

    @pytest.fixture
    def Table3(self, Table):
        class Tb(Table):
            name = Column()
        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        self._insert(Tb.__tablename__, [
            {'_key': 'key1', 'name': 'foo'},
            {'_key': 'key2', 'name': 'bar'},
            {'_key': 'key3', 'name': 'baz'},
            ])
        return Tb

    def test_default_value(self):
        Table = tablebase()
        assert Table.__tableflags__ is TableFlags.TABLE_HASH_KEY
        assert Table.__key_type__ is DataType.ShortText
        assert (Table.__tablename__ == 'Table') is True
        assert Table.__default_tokenizer__ is None
        assert Table.__normalizer__ is None

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

        assert Tb1.__tableflags__ is TableFlags.TABLE_HASH_KEY
        assert Tb1.__key_type__ is DataType.ShortText
        assert Tb1.__default_tokenizer__ is None
        assert Tb1.__normalizer__ is None
        assert (Tb1.__tablename__ == 'Tb1') is True
        assert str(Tb1) == ('table_create --name Tb1 --flags TABLE_HASH_KEY'
                            ' --key_type ShortText')
        assert (Tb1.columns == [namecol, passwordcol]) is True
        assert (Table._tables == [Tb1]) is True

        sitecol = Column(flags=ColumnFlags.COLUMN_SCALAR,
                         type=DataType.ShortText)
        addresscol = Column(flags=ColumnFlags.COLUMN_SCALAR,
                            type=DataType.ShortText)

        class Tb2(Table):
            __tableflags__ = TableFlags.TABLE_PAT_KEY
            __key_type__ = DataType.UInt32
            __default_tokenizer__ = Tokenizer.TokenBigram
            __normalizer__ = Normalizer.NormalizerAuto
            site = sitecol
            address = addresscol

        assert Tb2.__tableflags__ is TableFlags.TABLE_PAT_KEY
        assert Tb2.__key_type__ is DataType.UInt32
        assert (Tb2.__tablename__ == 'Tb2') is True
        assert str(Tb2) == ('table_create --name Tb2 --flags TABLE_PAT_KEY'
                            ' --key_type UInt32 --default_tokenizer'
                            ' TokenBigram --normalizer NormalizerAuto')
        assert (Tb2.columns == [sitecol, addresscol]) is True
        assert (Table._tables == [Tb1, Tb2]) is True

        class Tb3(Table):
            __tableflags__ = TableFlags.TABLE_NO_KEY
            site = sitecol
            address = addresscol

        assert Tb3.__tableflags__ is TableFlags.TABLE_NO_KEY
        assert (Tb3.__tablename__ == 'Tb3') is True
        assert str(Tb3) == 'table_create --name Tb3 --flags TABLE_NO_KEY'
        assert (Tb3.columns == [sitecol, addresscol]) is True
        assert (Table._tables == [Tb1, Tb2, Tb3]) is True

    def test_bind(self, Table):
        with pytest.raises(TypeError):
            Table.bind('dummy')
        grn = Groonga()
        Table.bind(grn)
        assert Table.grn is grn

    def test_create_all(self, Table):
        class Tb1(Table):
            name = Column(flags=ColumnFlags.COLUMN_SCALAR,
                          type=DataType.ShortText)

        class Tb2(Table):
            word = Column(flags=ColumnFlags.COLUMN_SCALAR,
                          type=DataType.ShortText)

        grn = Groonga()
        Table.bind(grn)
        Table.create_all()

        tableinfo = self.get_tableinfo(Tb1.__tablename__, Tb2.__tablename__)
        assert len(tableinfo) == 2
        assert tableinfo[0]['name'] == Tb1.__tablename__
        assert tableinfo[0]['flags'] == 'TABLE_HASH_KEY|PERSISTENT'
        assert tableinfo[0]['domain'] == 'ShortText'
        assert tableinfo[0]['range'] is None
        assert tableinfo[0]['default_tokenizer'] is None
        assert tableinfo[0]['normalizer'] is None
        assert tableinfo[1]['name'] == Tb2.__tablename__
        assert tableinfo[1]['flags'] == 'TABLE_HASH_KEY|PERSISTENT'
        assert tableinfo[1]['domain'] == 'ShortText'
        assert tableinfo[1]['range'] is None
        assert tableinfo[1]['default_tokenizer'] is None
        assert tableinfo[1]['normalizer'] is None

        columninfo = self.get_columninfo(Tb1.__tablename__)
        assert len(columninfo) == 2
        assert columninfo[0]['name'] == '_key'
        assert columninfo[0]['type'] == ''
        assert columninfo[0]['flags'] == 'COLUMN_SCALAR'
        assert columninfo[0]['domain'] == Tb1.__tablename__
        assert columninfo[0]['range'] == 'ShortText'
        assert columninfo[0]['source'] == []
        assert columninfo[1]['name'] == 'name'
        assert columninfo[1]['type'] == 'var'
        assert columninfo[1]['flags'] == 'COLUMN_SCALAR|PERSISTENT'
        assert columninfo[1]['domain'] == Tb1.__tablename__
        assert columninfo[1]['range'] == 'ShortText'
        assert columninfo[1]['source'] == []

        columninfo = self.get_columninfo(Tb2.__tablename__)
        assert len(columninfo) == 2
        assert columninfo[0]['name'] == '_key'
        assert columninfo[0]['type'] == ''
        assert columninfo[0]['flags'] == 'COLUMN_SCALAR'
        assert columninfo[0]['domain'] == Tb2.__tablename__
        assert columninfo[0]['range'] == 'ShortText'
        assert columninfo[0]['source'] == []
        assert columninfo[1]['name'] == 'word'
        assert columninfo[1]['type'] == 'var'
        assert columninfo[1]['flags'] == 'COLUMN_SCALAR|PERSISTENT'
        assert columninfo[1]['domain'] == Tb2.__tablename__
        assert columninfo[1]['range'] == 'ShortText'
        assert columninfo[1]['source'] == []

    def test_create_all_with_multiple_calls(self, Table):
        class Tb1(Table):
            name = Column(flags=ColumnFlags.COLUMN_SCALAR,
                          type=DataType.ShortText)

        class Tb2(Table):
            word = Column(flags=ColumnFlags.COLUMN_SCALAR,
                          type=DataType.ShortText)

        grn = Groonga()
        Table.bind(grn)
        Table.create_all()

        tableinfo = self.get_tableinfo(Tb1.__tablename__, Tb2.__tablename__)
        assert len(tableinfo) == 2
        assert tableinfo[0]['name'] == Tb1.__tablename__
        assert tableinfo[0]['flags'] == 'TABLE_HASH_KEY|PERSISTENT'
        assert tableinfo[0]['domain'] == 'ShortText'
        assert tableinfo[0]['range'] is None
        assert tableinfo[0]['default_tokenizer'] is None
        assert tableinfo[0]['normalizer'] is None
        assert tableinfo[1]['name'] == Tb2.__tablename__
        assert tableinfo[1]['flags'] == 'TABLE_HASH_KEY|PERSISTENT'
        assert tableinfo[1]['domain'] == 'ShortText'
        assert tableinfo[1]['range'] is None
        assert tableinfo[1]['default_tokenizer'] is None
        assert tableinfo[1]['normalizer'] is None

        columninfo1 = self.get_columninfo(Tb1.__tablename__)
        assert len(columninfo1) == 2
        assert columninfo1[0]['name'] == '_key'
        assert columninfo1[0]['type'] == ''
        assert columninfo1[0]['flags'] == 'COLUMN_SCALAR'
        assert columninfo1[0]['domain'] == Tb1.__tablename__
        assert columninfo1[0]['range'] == 'ShortText'
        assert columninfo1[0]['source'] == []
        assert columninfo1[1]['name'] == 'name'
        assert columninfo1[1]['type'] == 'var'
        assert columninfo1[1]['flags'] == 'COLUMN_SCALAR|PERSISTENT'
        assert columninfo1[1]['domain'] == Tb1.__tablename__
        assert columninfo1[1]['range'] == 'ShortText'
        assert columninfo1[1]['source'] == []

        columninfo2 = self.get_columninfo(Tb2.__tablename__)
        assert len(columninfo2) == 2
        assert columninfo2[0]['name'] == '_key'
        assert columninfo2[0]['type'] == ''
        assert columninfo2[0]['flags'] == 'COLUMN_SCALAR'
        assert columninfo2[0]['domain'] == Tb2.__tablename__
        assert columninfo2[0]['range'] == 'ShortText'
        assert columninfo2[0]['source'] == []
        assert columninfo2[1]['name'] == 'word'
        assert columninfo2[1]['type'] == 'var'
        assert columninfo2[1]['flags'] == 'COLUMN_SCALAR|PERSISTENT'
        assert columninfo2[1]['domain'] == Tb2.__tablename__
        assert columninfo2[1]['range'] == 'ShortText'
        assert columninfo2[1]['source'] == []

        # again
        Table.create_all()

        tableinfo = self.get_tableinfo(Tb1.__tablename__, Tb2.__tablename__)
        assert len(tableinfo) == 2
        assert tableinfo[0]['name'] == Tb1.__tablename__
        assert tableinfo[0]['flags'] == 'TABLE_HASH_KEY|PERSISTENT'
        assert tableinfo[0]['domain'] == 'ShortText'
        assert tableinfo[0]['range'] is None
        assert tableinfo[0]['default_tokenizer'] is None
        assert tableinfo[0]['normalizer'] is None
        assert tableinfo[1]['name'] == Tb2.__tablename__
        assert tableinfo[1]['flags'] == 'TABLE_HASH_KEY|PERSISTENT'
        assert tableinfo[1]['domain'] == 'ShortText'
        assert tableinfo[1]['range'] is None
        assert tableinfo[1]['default_tokenizer'] is None
        assert tableinfo[1]['normalizer'] is None

        columninfo1 = self.get_columninfo(Tb1.__tablename__)
        assert len(columninfo1) == 2
        assert columninfo1[0]['name'] == '_key'
        assert columninfo1[0]['type'] == ''
        assert columninfo1[0]['flags'] == 'COLUMN_SCALAR'
        assert columninfo1[0]['domain'] == Tb1.__tablename__
        assert columninfo1[0]['range'] == 'ShortText'
        assert columninfo1[0]['source'] == []
        assert columninfo1[1]['name'] == 'name'
        assert columninfo1[1]['type'] == 'var'
        assert columninfo1[1]['flags'] == 'COLUMN_SCALAR|PERSISTENT'
        assert columninfo1[1]['domain'] == Tb1.__tablename__
        assert columninfo1[1]['range'] == 'ShortText'
        assert columninfo1[1]['source'] == []

        columninfo2 = self.get_columninfo(Tb2.__tablename__)
        assert len(columninfo2) == 2
        assert columninfo2[0]['name'] == '_key'
        assert columninfo2[0]['type'] == ''
        assert columninfo2[0]['flags'] == 'COLUMN_SCALAR'
        assert columninfo2[0]['domain'] == Tb2.__tablename__
        assert columninfo2[0]['range'] == 'ShortText'
        assert columninfo2[0]['source'] == []
        assert columninfo2[1]['name'] == 'word'
        assert columninfo2[1]['type'] == 'var'
        assert columninfo2[1]['flags'] == 'COLUMN_SCALAR|PERSISTENT'
        assert columninfo2[1]['domain'] == Tb2.__tablename__
        assert columninfo2[1]['range'] == 'ShortText'
        assert columninfo2[1]['source'] == []

    def test_select_all(self, Table):
        class Tb(Table):
            pass

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY '
                        '--key_type ShortText' % Tb.__tablename__)
        self._insert(Tb.__tablename__, [
            {'_key': 'key1'},
            {'_key': 'key2'},
            {'_key': u'キー３'},
            {'_key': 'キー４'},
            ])
        result = Tb.select().all()
        expected = [{'_id': 1, '_key': 'key1'},
                    {'_id': 2, '_key': 'key2'},
                    {'_id': 3, '_key': u'キー３'},
                    {'_id': 4, '_key': u'キー４'}]
        self.assertGroongaResultEqual(result, expected, all_len=4)

    def test_select_with_column(self, Table):
        class Tb(Table):
            name = Column()
            address = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY '
                        '--key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags '
                        'COLUMN_SCALAR --type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name address --flags '
                        'COLUMN_SCALAR --type ShortText' % Tb.__tablename__)
        self._insert(Tb.__tablename__, [
            {'_key': 'key1', 'name': 'Name1', 'address': 'Address1'},
            {'_key': 'key2', 'name': 'name2', 'address': 'address2'},
            {'_key': 'key3', 'name': 'foo', 'address': 'bar'},
            {'_key': u'キー４', 'name': '園城寺怜', 'address': u'千里山'},
            {'_key': 'キー５', 'name': u'白水哩', 'address': '新道寺'},
            ])

        result = Tb.select(_key='1').all()
        expected = [{'_id': 1, '_key': 'key1', 'address': 'Address1',
                     'name': 'Name1'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(_key='2').all()
        expected = [{'_id': 2, '_key': 'key2', 'address': 'address2',
                     'name': 'name2'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(_key='キー４').all()
        expected = [{'_id': 4, '_key': u'キー４', 'address': u'千里山',
                     'name': u'園城寺怜'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(_key=u'キー５').all()
        expected = [{'_id': 5, '_key': u'キー５', 'address': u'新道寺',
                     'name': u'白水哩'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(name='name').all()
        expected = [{'_id': 1, '_key': 'key1', 'address': 'Address1',
                     'name': 'Name1'},
                    {'_id': 2, '_key': 'key2', 'address': 'address2',
                     'name': 'name2'},
                    ]
        self.assertGroongaResultEqual(result, expected, all_len=2)

        result = Tb.select(name='城').all()
        expected = [{'_id': 4, '_key': u'キー４', 'address': u'千里山',
                     'name': u'園城寺怜'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(address='ar').all()
        expected = [{'_id': 3, '_key': 'key3', 'address': 'bar',
                     'name': 'foo'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

        result = Tb.select(address='道寺').all()
        expected = [{'_id': 5, '_key': u'キー５', 'address': u'新道寺',
                     'name': u'白水哩'}]
        self.assertGroongaResultEqual(result, expected, all_len=1)

    def test_select_with_condition(self, Table1, fixture1):
        Tb, fixture = Table1, fixture1

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

    def test_select_with_limit(self, Table1, fixture1):
        Tb, fixture = Table1, fixture1

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

    def test_select_with_offset(self, Table1, fixture1):
        Tb, fixture = Table1, fixture1

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

    def test_select_with_sortby(self, Table1, fixture1):
        Tb, fixture = Table1, fixture1

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

    def test_select_with_output_columns(self, Table1, fixture1):
        Tb, fixture = Table1, fixture1

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

    def test_select_with_drilldown(self, Table2, fixture2):
        Tb, fixture = Table2, fixture2

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

    def test_select_with_drilldown_sortby(self, Table2, fixture2):
        Tb, fixture = Table2, fixture2

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

    def test_select_with_drilldown_output_columns(self, Table2, fixture2):
        Tb, fixture = Table2, fixture2

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

    def test_select_with_drilldown_offset(self, Table2, fixture2):
        Tb, fixture = Table2, fixture2

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

    def test_select_with_drilldown_limit(self, Table2, fixture2):
        Tb, fixture = Table2, fixture2

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

    def test_load_immediately(self, Table):
        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)

        data = [Tb(_key='key1', name='name1'),
                Tb(_key='key2', name='name2'),
                Tb(_key='key3', name='name3'),
                Tb(_key='キー４', name='園城寺怜'),
                Tb(_key=u'キー５', name=u'白水哩')]
        result = Tb.load(data)
        assert (result == 5) is True
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[5],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'name1'],
                     [2, 'key2', 'name2'],
                     [3, 'key3', 'name3'],
                     [4, u'キー４', u'園城寺怜'],
                     [5, u'キー５', u'白水哩']]]
        assert stored[1] == expected

        # data override test
        data = [Tb(_key='key1', name='foo'),
                Tb(_key='key2', name='bar'),
                Tb(_key='key3', name='baz'),
                Tb(_key='キー４', name='松実玄'),
                Tb(_key=u'キー５', name='新子憧')]
        result = Tb.load(data)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[5],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'foo'],
                     [2, 'key2', 'bar'],
                     [3, 'key3', 'baz'],
                     [4, u'キー４', u'松実玄'],
                     [5, u'キー５', u'新子憧']]]
        assert stored[1] == expected

    def test_load_lazy(self, Table):
        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)

        data = [Tb(_key='key1', name='name1'),
                Tb(_key='key2', name='name2'),
                Tb(_key='key3', name='name3'),
                Tb(_key='キー４', name='園城寺怜'),
                Tb(_key=u'キー５', name=u'白水哩')]
        result = Tb.load(data, immediate=False)
        assert isinstance(result, LoadQuery)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected
        result.commit()  # load actually
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[5],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'name1'],
                     [2, 'key2', 'name2'],
                     [3, 'key3', 'name3'],
                     [4, u'キー４', u'園城寺怜'],
                     [5, u'キー５', u'白水哩']]]
        assert stored[1] == expected

        # duplicate commit
        with pytest.raises(RuntimeError):
            result.commit()

    def test_load_lazy_multiple(self, Table):
        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)

        data1 = [Tb(_key='key1', name='name1'),
                 Tb(_key='key2', name='name2'),
                 Tb(_key='key3', name='name3')]
        result = Tb.load(data1, immediate=False)
        assert isinstance(result, LoadQuery)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected
        data2 = [Tb(_key='key4', name='Madoka Kaname'),
                 Tb(_key='key5', name='Homura Akemi'),
                 Tb(_key='key6', name='Kyoko Sakura'),
                 Tb(_key='key7', name='Sayaka Miki'),
                 Tb(_key=u'キー８', name=u'巴マミ')]
        result.load(data2)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected
        result.commit()  # load actually
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[8],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'key1', 'name1'],
                     [2, 'key2', 'name2'],
                     [3, 'key3', 'name3'],
                     [4, 'key4', 'Madoka Kaname'],
                     [5, 'key5', 'Homura Akemi'],
                     [6, 'key6', 'Kyoko Sakura'],
                     [7, 'key7', 'Sayaka Miki'],
                     [8, u'キー８', u'巴マミ']]]
        assert stored[1] == expected

    def test_load_lazy_rollback(self, Table):
        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)

        data1 = [Tb(_key='key1', name='name1'),
                 Tb(_key='key2', name='name2'),
                 Tb(_key='key3', name='name3')]
        result = Tb.load(data1, immediate=False)
        result.rollback()
        with pytest.raises(RuntimeError):
            result.commit()
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected

    def test_delete_with_mapped_object_immediate(self, Table):
        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        test_utils.sendquery(r'load --table %s --input_type json --values'
                             r' [{\"_key\":\"foo\",\"name\":\"bar\"},'
                             r'{\"_key\":\"bar\",\"name\":\"baz\"}]' %
                             Tb.__tablename__)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[2],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'foo', 'bar'], [2, 'bar', 'baz']]]
        assert stored[1] == expected
        results = Tb.select().all()
        assert len(results) == 2
        result = results[0]
        assert result.delete() is True
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[1],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [2, 'bar', 'baz']]]
        assert stored[1] == expected
        assert result.delete() is False
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[1],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [2, 'bar', 'baz']]]
        assert stored[1] == expected
        result = results[1]
        assert result.delete() is True
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected
        assert result.delete() is False
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected

    def test_delete_with_mapped_object_and_not_immediate(self, Table):
        class Tb(Table):
            name = Column()

        grn = Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        test_utils.sendquery(r'load --table %s --input_type json --values'
                             r' [{\"_key\":\"foo\",\"name\":\"bar\"},'
                             r'{\"_key\":\"bar\",\"name\":\"baz\"}]' %
                             Tb.__tablename__)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[2],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'foo', 'bar'], [2, 'bar', 'baz']]]
        assert stored[1] == expected
        results = Tb.select().all()
        assert len(results) == 2
        result = results[0].delete(immediate=False)
        assert isinstance(result, query.SimpleQuery)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[2],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [1, 'foo', 'bar'], [2, 'bar', 'baz']]]
        assert stored[1] == expected
        assert result.execute() is True
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[1],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [2, 'bar', 'baz']]]
        assert stored[1] == expected
        assert result.execute() is False
        result = results[1].delete(immediate=False)
        assert isinstance(result, query.SimpleQuery)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[1],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']],
                     [2, 'bar', 'baz']]]
        assert stored[1] == expected
        assert result.execute() is True
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected
        assert result.execute() is False
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Tb.__tablename__))
        expected = [[[0],
                     [['_id', 'UInt32'], ['_key', 'ShortText'],
                      ['name', 'ShortText']]]]
        assert stored[1] == expected

    delete_params_for_valid = (
        ({'key': 'key2'}, [
            True,
            [[[2], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
            [1, 'key1', 'foo'], [3, 'key3', 'baz']]]]),
        ({'key': 'key1'}, [
            True,
            [[[2], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
            [2, 'key2', 'bar'], [3, 'key3', 'baz']]]]),
        ({'key': 'key4'}, [
            False,
            [[[3], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
            [1, 'key1', 'foo'], [2, 'key2', 'bar'],
            [3, 'key3', 'baz']]]]),
        ({'id': '1'}, [
            True,
            [[[2], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
            [2, 'key2', 'bar'], [3, 'key3', 'baz']]]]),
        ({'id': 3}, [
            True,
            [[[2], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
            [1, 'key1', 'foo'], [2, 'key2', 'bar']]]]),
        ({'filter': 'name@"baz"'}, [
            True,
            [[[2], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
            [1, 'key1', 'foo'], [2, 'key2', 'bar']]]]),
        )

    delete_params_for_invalid = (
        ({'key': 'key1', 'id': 1}, [
            False,
            [[[3], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
              [1, 'key1', 'foo'], [2, 'key2', 'bar'],
              [3, 'key3', 'baz']]]]),
        ({'key': 'key2', 'filter': 'name@"bar"'}, [
            False,
            [[[3], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
              [1, 'key1', 'foo'], [2, 'key2', 'bar'],
              [3, 'key3', 'baz']]]]),
        ({'id': '1', 'filter': 'name@"bar"'}, [
            False,
            [[[3], [['_id', 'UInt32'], ['_key', 'ShortText'],
                    ['name', 'ShortText']],
              [1, 'key1', 'foo'], [2, 'key2', 'bar'],
              [3, 'key3', 'baz']]]]),
        )

    @pytest.mark.parametrize(('cond', 'expected'), delete_params_for_valid)
    def test_delete_with_immediate(self, Table3, cond, expected):
        assert Table3.delete(**cond) is expected[0]
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Table3.__tablename__))
        assert stored[1] == expected[1]

    @pytest.mark.parametrize(('cond', 'expected'), delete_params_for_invalid)
    def test_delete_with_immediate_and_invalid_params(
            self, Table3, cond, expected):
        with pytest.raises(exceptions.GroongaError):
            Table3.delete(**cond)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Table3.__tablename__))
        assert stored[1] == expected[1]

    @pytest.mark.parametrize(('cond', 'expected'), delete_params_for_valid)
    def test_delete_with_not_immediate(self, Table3, cond, expected):
        q = Table3.delete(immediate=False, **cond)
        assert isinstance(q, query.SimpleQuery)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Table3.__tablename__))
        noeffect_expected = [[[3], [['_id', 'UInt32'], ['_key', 'ShortText'],
                                    ['name', 'ShortText']],
                              [1, 'key1', 'foo'], [2, 'key2', 'bar'],
                              [3, 'key3', 'baz']]]
        assert stored[1] == noeffect_expected
        assert q.execute() is expected[0]
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Table3.__tablename__))
        assert stored[1] == expected[1]

    @pytest.mark.parametrize(('cond', 'expected'), delete_params_for_invalid)
    def test_delete_with_not_immediate_and_invalid_params(
            self, Table3, cond, expected):
        q = Table3.delete(immediate=False, **cond)
        assert isinstance(q, query.SimpleQuery)
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Table3.__tablename__))
        noeffect_expected = [[[3], [['_id', 'UInt32'], ['_key', 'ShortText'],
                                    ['name', 'ShortText']],
                              [1, 'key1', 'foo'], [2, 'key2', 'bar'],
                              [3, 'key3', 'baz']]]
        assert stored[1] == noeffect_expected
        with pytest.raises(exceptions.GroongaError):
            q.execute()
        stored = json.loads(test_utils.sendquery(
            'select --table %s' % Table3.__tablename__))
        assert stored[1] == expected[1]

    def test_truncate_with_default_param(self, Table1, Table2, Table3):
        def get_items_num(table):
            stored = json.loads(test_utils.sendquery(
                'select %s --limit 0' % table.__tablename__))
            return stored[1][0][0][0]
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 8
        assert get_items_num(Table3) == 3

        assert Table2.truncate() is True
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 3

        assert Table3.truncate() is True
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 0

    def test_truncate_with_immediate(self, Table1, Table2, Table3):
        def get_items_num(table):
            stored = json.loads(test_utils.sendquery(
                'select %s --limit 0' % table.__tablename__))
            return stored[1][0][0][0]
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 8
        assert get_items_num(Table3) == 3

        assert Table2.truncate(immediate=True) is True
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 3

        assert Table3.truncate(immediate=True) is True
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 0

    def test_truncate_with_not_immediate(self, Table1, Table2, Table3):
        def get_items_num(table):
            stored = json.loads(test_utils.sendquery(
                'select %s --limit 0' % table.__tablename__))
            return stored[1][0][0][0]
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 8
        assert get_items_num(Table3) == 3

        q = Table2.truncate(immediate=False)
        assert isinstance(q, query.SimpleQuery)
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 8
        assert get_items_num(Table3) == 3
        assert q.execute() is True
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 3

        q = Table3.truncate(immediate=False)
        assert isinstance(q, query.SimpleQuery)
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 3
        assert q.execute() is True
        assert get_items_num(Table1) == 5
        assert get_items_num(Table2) == 0
        assert get_items_num(Table3) == 0

    def test_cache_limit_with_default_and_no_param(self, Table):
        Table.bind(Groonga())
        lim = json.loads(test_utils.sendquery('cache_limit'))[1]
        assert lim == 100
        result = Table.cache_limit()
        assert result == lim

    def test_cache_limit_with_immediate_and_no_param(self, Table):
        Table.bind(Groonga())
        lim = json.loads(test_utils.sendquery('cache_limit'))[1]
        assert lim == 100
        result = Table.cache_limit(immediate=True)
        assert result == lim

    def test_cache_limit_with_not_immediate_and_no_param(self, Table):
        Table.bind(Groonga())
        lim = json.loads(test_utils.sendquery('cache_limit'))[1]
        assert lim == 100
        q = Table.cache_limit(immediate=False)
        assert isinstance(q, query.SimpleQuery)
        result = q.execute()
        assert result == lim

    def test_cache_limit_with_default_and_param(self, request, Table):
        request.addfinalizer(lambda: test_utils.sendquery('cache_limit 100'))

        def get_limit():
            return json.loads(test_utils.sendquery('cache_limit'))[1]
        Table.bind(Groonga())
        lim = get_limit()
        assert lim == 100
        expected_limit = random.randint(101, 500)
        result = Table.cache_limit(expected_limit)
        assert result == lim
        assert get_limit() == expected_limit

    def test_cache_limit_with_immediate_and_param(self, request, Table):
        request.addfinalizer(lambda: test_utils.sendquery('cache_limit 100'))

        def get_limit():
            return json.loads(test_utils.sendquery('cache_limit'))[1]
        Table.bind(Groonga())
        lim = get_limit()
        assert lim == 100
        expected_limit = random.randint(101, 500)
        result = Table.cache_limit(expected_limit, immediate=True)
        assert result == lim
        assert get_limit() == expected_limit

    def test_cache_limit_with_not_immediate_and_param(self, request, Table):
        request.addfinalizer(lambda: test_utils.sendquery('cache_limit 100'))

        def get_limit():
            return json.loads(test_utils.sendquery('cache_limit'))[1]
        Table.bind(Groonga())
        lim = get_limit()
        assert lim == 100
        expected_limit = random.randint(101, 500)
        q = Table.cache_limit(expected_limit, immediate=False)
        assert isinstance(q, query.SimpleQuery)
        assert get_limit() == lim
        result = q.execute()
        assert result == lim
        assert get_limit() == expected_limit

    log_level_params = [v for v in attributes.LogLevel.__dict__.values()
                        if isinstance(v, attributes.LogLevelSymbol)]

    @pytest.mark.parametrize('level', log_level_params)
    def test_log_level_with_default(self, Table, level):
        Table.bind(Groonga())
        result = Table.log_level(level)
        assert result is True

    @pytest.mark.parametrize('level', log_level_params)
    def test_log_level_with_immediate(self, Table, level):
        Table.bind(Groonga())
        result = Table.log_level(level, immediate=True)
        assert result is True

    @pytest.mark.parametrize('level', log_level_params)
    def test_log_level_with_not_immediate(self, Table, level):
        Table.bind(Groonga())
        q = Table.log_level(level, immediate=False)
        assert isinstance(q, query.SimpleQuery)
        result = q.execute()
        assert result is True

    @pytest.mark.parametrize('level', log_level_params)
    def test_log_put_with_default(self, Table, level):
        Table.bind(Groonga())
        expected = test_utils.random_string()
        result = Table.log_put(level, expected)
        assert result is True
        # TODO: log output check actually

    @pytest.mark.parametrize('level', log_level_params)
    def test_log_put_with_immediate(self, Table, level):
        Table.bind(Groonga())
        expected = test_utils.random_string()
        result = Table.log_put(level, expected, immediate=True)
        assert result is True
        # TODO: log output check actually

    @pytest.mark.parametrize('level', log_level_params)
    def test_log_put_with_not_immediate(self, Table, level):
        Table.bind(Groonga())
        expected = test_utils.random_string()
        q = Table.log_put(level, expected, immediate=False)
        assert isinstance(q, query.SimpleQuery)
        result = q.execute()
        assert result is True
        # TODO: log output check actually

    def test_log_reopen_with_default(self, Table):
        Table.bind(Groonga())
        result = Table.log_reopen()
        assert result is True

    def test_log_reopen_with_immediate(self, Table):
        Table.bind(Groonga())
        result = Table.log_reopen(immediate=True)
        assert result is True

    def test_log_reopen_with_not_immediate(self, Table):
        Table.bind(Groonga())
        q = Table.log_reopen(immediate=False)
        assert isinstance(q, query.SimpleQuery)
        result = q.execute()
        assert result is True

    def test_asdict(self, Table):
        class A(Table):
            foo = None
            bar = None
            baz = None
        expected1 = test_utils.random_string()
        expected2 = '園城寺怜'
        expected3 = u'白水哩'
        table = A(foo=expected1, bar=expected2, baz=expected3)
        result = table.asdict()
        assert result == {'foo': expected1, 'bar': expected2, 'baz': expected3}

    @pytest.mark.parametrize(('excludes', 'expected'), (
        (['foo'], {'bar': '園城寺怜', 'baz': u'白水哩'}),
        (['bar'], {'foo': 'bar', 'baz': u'白水哩'}),
        (['foo', 'baz'], {'bar': '園城寺怜'}),
    ))
    def test_asdict_with_excludes(self, Table, excludes, expected):
        class A(Table):
            foo = None
            bar = None
            baz = None
        table = A(foo='bar', bar='園城寺怜', baz=u'白水哩')
        result = table.asdict(excludes=excludes)
        assert result == expected


@pytest.mark.xfail(reason=(
    "failed to parallel tests due to fixed table names."
    " It will failed on at least travis-ci."
))
class TestSuggestTable(object):
    def _load(self, count):
        fixture = self.loadfixture('_suggest')
        data = json.dumps(fixture)
        for i in range(count):
            test_utils.sendquery(
                "load --table event_query --input_type json --each"
                " 'suggest_preparer(_id, type, item, sequence,"
                " time, pair_query)'\n%s" % (data))

    def test_create_all(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()
        result = json.loads(test_utils.sendquery('table_list'))
        expected = [[['id', 'UInt32'],
                     ['name', 'ShortText'],
                     ['path', 'ShortText'],
                     ['flags', 'ShortText'],
                     ['domain', 'ShortText'],
                     ['range', 'ShortText'],
                     ['default_tokenizer', 'ShortText'],
                     ['normalizer', 'ShortText']],
                    [259, 'bigram', GroongaTestBase.DB_PATH + '.0000103',
                     'TABLE_PAT_KEY|PERSISTENT',
                     'ShortText',
                     None,
                     'TokenBigram',
                     None],
                    [264, 'event_query', GroongaTestBase.DB_PATH + '.0000108',
                     'TABLE_NO_KEY|PERSISTENT',
                     None,
                     None,
                     None,
                     None],
                    [258, 'event_type', GroongaTestBase.DB_PATH + '.0000102',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'ShortText',
                     None,
                     None,
                     None],
                    [261, 'item_query', GroongaTestBase.DB_PATH + '.0000105',
                     'TABLE_PAT_KEY|PERSISTENT',
                     'ShortText',
                     None,
                     'TokenDelimit',
                     None],
                    [262, 'kana', GroongaTestBase.DB_PATH + '.0000106',
                     'TABLE_PAT_KEY|PERSISTENT',
                     'ShortText',
                     None,
                     None,
                     None],
                    [260, 'pair_query', GroongaTestBase.DB_PATH + '.0000104',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'UInt64',
                     None,
                     None,
                     None],
                    [263, 'sequence_query',
                     GroongaTestBase.DB_PATH + '.0000107',
                     'TABLE_HASH_KEY|PERSISTENT',
                     'ShortText',
                     None,
                     None,
                     None],
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
        stored = json.loads(test_utils.sendquery('select --table event_query'))
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
        stored = json.loads(test_utils.sendquery('select --table event_query'))
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

        stored = json.loads(test_utils.sendquery('select --table item_query'))
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
        except Exception:
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
            'suggest --table "item_query" --column '
            '"kana" --types "complete" '
            '--conditional_probability_threshold 1.0 --query "en"')

        query = item_query.suggest('en'). \
            conditional_probability_threshold(0.2)
        self.assertEqual(str(query),
            'suggest --table "item_query" --column '
            '"kana" --types "complete" '
            '--conditional_probability_threshold 0.2 --query "en"')

    def test_suggest_with_prefix_search(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()

        query = item_query.suggest('en').prefix_search(True)
        self.assertEqual(str(query),
            'suggest --table "item_query" --column '
            '"kana" --types "complete" --prefix_search yes --query "en"')

        query = item_query.suggest('en').prefix_search(False)
        self.assertEqual(str(query),
            'suggest --table "item_query" --column '
            '"kana" --types "complete" --prefix_search no --query "en"')

    def test_suggest_with_similar_search(self):
        grn = Groonga()
        SuggestTable.bind(grn)
        SuggestTable.create_all()

        query = item_query.suggest('en').similar_search(True)
        self.assertEqual(str(query),
            'suggest --table "item_query" --column '
            '"kana" --types "complete" --similar_search yes --query "en"')

        query = item_query.suggest('en').similar_search(False)
        self.assertEqual(str(query),
            'suggest --table "item_query" --column '
            '"kana" --types "complete" --similar_search no --query "en"')
