import unittest
from nose import SkipTest
import os
if os.environ.get('DB') != 'mysql':
    raise SkipTest("MySQL not under test")
try:
    import MySQLdb
except ImportError:
    raise SkipTest("MySQLdb not found, skipping MySQL tests")
import logging
_logger = logging.getLogger(__name__)
import context_case
import graph_case
from rdflib import Literal

# Specific to Travis-ci continuous integration and testing ...
sqlalchemy_url = Literal(os.environ.get(
    'DBURI',
    "mysql://root@127.0.0.1:3306/rdflibsqla_test"))
# Generally ...
# sqlalchemy_url = Literal(
#    "mysql+mysqldb://user:password@hostname:port/database?charset=utf8")


class SQLAMySQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def setUp(self):
        graph_case.GraphTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)

    def testStatementNode(self):
        raise SkipTest("Known issue.")


class SQLAMySQLContextTestCase(context_case.ContextTestCase):
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

SQLAMySQLGraphTestCase.storetest = True
SQLAMySQLContextTestCase.storetest = True

if __name__ == '__main__':
    unittest.main()
