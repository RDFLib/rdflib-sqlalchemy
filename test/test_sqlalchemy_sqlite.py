import logging
import os
import unittest

from nose import SkipTest
from rdflib import Literal

from . import context_case
from . import graph_case


if os.environ.get("DB") != "sqlite":
    raise SkipTest("SQLite not under test")

_logger = logging.getLogger(__name__)

sqlalchemy_url = Literal(os.environ.get("DBURI", "sqlite://"))


class SQLASQLiteGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def setUp(self):
        super(SQLASQLiteGraphTestCase, self).setUp(
            uri=self.uri, storename=self.storename)

    def tearDown(self):
        super(SQLASQLiteGraphTestCase, self).tearDown(uri=self.uri)


class SQLASQLiteContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def setUp(self):
        super(SQLASQLiteContextTestCase, self).setUp(
            uri=self.uri, storename=self.storename)

    def tearDown(self):
        super(SQLASQLiteContextTestCase, self).tearDown(uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")


SQLASQLiteGraphTestCase.storetest = True
SQLASQLiteContextTestCase.storetest = True


if __name__ == "__main__":
    unittest.main()
