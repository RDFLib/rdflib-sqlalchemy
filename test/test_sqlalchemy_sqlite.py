from nose.exc import SkipTest
import os
if os.environ.get('DB') != 'sqlite':
    raise SkipTest("SQLite not under test")
import unittest
import logging
_logger = logging.getLogger(__name__)
import context_case
import graph_case
from rdflib import Literal

sqlalchemy_url = Literal(os.environ.get('DBURI',"sqlite://"))


class SQLASQLiteGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def setUp(self):
        graph_case.GraphTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)


class SQLASQLiteContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def setUp(self):
        context_case.ContextTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        context_case.ContextTestCase.tearDown(self, uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")

SQLASQLiteGraphTestCase.storetest = True
SQLASQLiteContextTestCase.storetest = True


if __name__ == '__main__':
    unittest.main()
