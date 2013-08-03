# -*- coding: utf-8 -*-

import pytest

from pyroonga.odm.table import tablebase, TableBase

from pyroonga.tests import utils


@pytest.fixture
def Table(request):
    class TableBaseForTest(TableBase):
        @utils.classproperty
        def __tablename__(cls):
            if not getattr(cls, '_tablename', None):
                cls._tablename = utils.gen_unique_tablename()
            return cls._tablename
    Tbl = tablebase(cls=TableBaseForTest)

    def remove_table():
        utils.sendquery('table_remove %s' % Tbl.__tablename__)
    request.addfinalizer(remove_table)
    return Tbl
