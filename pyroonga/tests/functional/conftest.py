# -*- coding: utf-8 -*-

import json
import os

import pytest

from pyroonga.odm.table import tablebase, TableBase

from pyroonga.tests import utils

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'fixture')
FIXTURE_PATH = os.path.join(FIXTURE_DIR, 'dbfixture%s.json')


@pytest.fixture
def Table(request):
    class TableBaseForTest(TableBase):
        @utils.classproperty
        def __tablename__(cls):
            if not getattr(cls, '_tablename', None):
                cls._tablename = utils.gen_unique_tablename()

                def remove_table():
                    utils.sendquery('table_remove %s' % cls._tablename)
                request.addfinalizer(remove_table)
            return cls._tablename
    Tbl = tablebase(cls=TableBaseForTest)
    return Tbl


@pytest.fixture
def fixture1():
    with open(FIXTURE_PATH % 1) as f:
        return json.load(f)


@pytest.fixture
def fixture2():
    with open(FIXTURE_PATH % 2) as f:
        return json.load(f)
