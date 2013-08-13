# -*- coding: utf-8 -*-

import json

import pytest

import pyroonga
from pyroonga.odm import query, table

from pyroonga.tests import utils as test_utils


class TestGroongaRecord(object):
    @pytest.fixture
    def Table1(self, Table):
        class Tb(Table):
            name = table.Column()
        grn = pyroonga.Groonga()
        Table.bind(grn)
        test_utils.sendquery('table_create --name %s --flags TABLE_HASH_KEY'
                             ' --key_type ShortText' % Tb.__tablename__)
        test_utils.sendquery('column_create --table %s --name name --flags'
                             ' COLUMN_SCALAR --type ShortText' %
                             Tb.__tablename__)
        test_utils.insert(Tb.__tablename__, [
            {'_key': 'key1', 'name': 'foo'},
            {'_key': 'key2', 'name': 'bar'},
            {'_key': 'key3', 'name': 'baz'},
            ])
        return Tb

    def test_commit_with_not_changed(self, Table1):
        record1, record2, record3 = Table1.select().all()
        assert record1.name == 'foo'
        assert record2.name == 'bar'
        assert record3.name == 'baz'
        record1.commit()
        assert record1.name == 'foo'
        assert record2.name == 'bar'
        assert record3.name == 'baz'
        record2.commit()
        assert record1.name == 'foo'
        assert record2.name == 'bar'
        assert record3.name == 'baz'
        record3.commit()
        assert record1.name == 'foo'
        assert record2.name == 'bar'
        assert record3.name == 'baz'
        records = json.loads(test_utils.sendquery('select --table %s' %
                                                  Table1.__tablename__))
        assert records[1][0][2:] == [[1, 'key1', 'foo'], [2, 'key2', 'bar'],
                                     [3, 'key3', 'baz']]

    def test_commit(self, Table1):
        record1, record2, record3 = Table1.select().all()
        expected1, expected2, expected3 = [test_utils.random_string() for _ in
                                           range(3)]
        assert record1.name == 'foo'
        assert record2.name == 'bar'
        assert record3.name == 'baz'
        record1.name = expected1
        record1.commit()
        assert record1.name == expected1
        assert record2.name == 'bar'
        assert record3.name == 'baz'
        records = json.loads(test_utils.sendquery('select --table %s' %
                                                  Table1.__tablename__))
        assert records[1][0][2:] == [[1, 'key1', expected1],
                                     [2, 'key2', 'bar'],
                                     [3, 'key3', 'baz']]
        record2.name = expected2
        record2.commit()
        assert record1.name == expected1
        assert record2.name == expected2
        assert record3.name == 'baz'
        records = json.loads(test_utils.sendquery('select --table %s' %
                                                  Table1.__tablename__))
        assert records[1][0][2:] == [[1, 'key1', expected1],
                                     [2, 'key2', expected2],
                                     [3, 'key3', 'baz']]
        record3.name = expected3
        record3.commit()
        assert record1.name == expected1
        assert record2.name == expected2
        assert record3.name == expected3
        records = json.loads(test_utils.sendquery('select --table %s' %
                                                  Table1.__tablename__))
        assert records[1][0][2:] == [[1, 'key1', expected1],
                                     [2, 'key2', expected2],
                                     [3, 'key3', expected3]]
