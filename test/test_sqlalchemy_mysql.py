from nose import SkipTest
try:
    import MySQLdb
except ImportError:
    raise SkipTest("MySQLdb not found, skipping MySQL tests")
import logging
_logger = logging.getLogger(__name__)
import context_case
import graph_case
from rdflib import Literal

sqlalchemy_url = Literal("mysql://gjh:50uthf0rk@localhost:3306/test")
# sqlalchemy_url = Literal("mysql+mysqldb://user:password@hostname:port/database?charset=utf8")

class SQLAlchemyMySQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    def setUp(self):
        graph_case.GraphTestCase.setUp(self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)

    def testStatementNode(self):
        raise SkipTest("Known issue.")

class SQLAlchemyMySQLContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    def setUp(self):
        context_case.ContextTestCase.setUp(self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        context_case.ContextTestCase.tearDown(self, uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")

SQLAlchemyMySQLGraphTestCase.storetest = True
SQLAlchemyMySQLContextTestCase.storetest = True
