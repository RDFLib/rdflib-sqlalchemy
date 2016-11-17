import logging
import os
import unittest

from nose import SkipTest
try:
    import psycopg2  # noqa
except ImportError:
    raise SkipTest("psycopg2 not installed, skipping PgSQL tests")

from . import context_case
from . import graph_case


if os.environ.get("DB") != "pgsql":
    raise SkipTest("PgSQL not under test")

sqlalchemy_url = os.environ.get(
    "DBURI",
    "postgresql+psycopg2://postgres@localhost/test")

_logger = logging.getLogger(__name__)


class SQLAPgSQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLAPgSQLGraphTestCase, self).setUp(
            uri=self.uri,
            storename=self.storename,
        )

    def tearDown(self):
        super(SQLAPgSQLGraphTestCase, self).tearDown(uri=self.uri)


class SQLAPgSQLContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLAPgSQLContextTestCase, self).setUp(
            uri=self.uri,
            storename=self.storename,
        )

    def tearDown(self):
        super(SQLAPgSQLContextTestCase, self).tearDown(uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")


SQLAPgSQLGraphTestCase.storetest = True
SQLAPgSQLContextTestCase.storetest = True

if __name__ == "__main__":
    unittest.main()
