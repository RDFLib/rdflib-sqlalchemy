import os
import logging
_logger = logging.getLogger(__name__)
import context_case
import graph_case
from nose import SkipTest
from rdflib import Literal

sqlalchemy_url = Literal("sqlite://")

class SQLAlchemySQLiteGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    def setUp(self):
        graph_case.GraphTestCase.setUp(self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)

    def testStatementNode(self):
        raise SkipTest("Known issue.")

class SQLAlchemySQLiteContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    def setUp(self):
        context_case.ContextTestCase.setUp(self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        context_case.ContextTestCase.tearDown(self, uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")

SQLAlchemySQLiteGraphTestCase.storetest = True
SQLAlchemySQLiteContextTestCase.storetest = True
