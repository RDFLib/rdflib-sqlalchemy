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
from . import context_case
from . import graph_case
from rdflib import Literal

# Specific to Travis-ci continuous integration and testing ...
sqlalchemy_url = Literal(os.environ.get(
    'DBURI',
    "mysql://root@127.0.0.1:3306/rdflibsqla_test?charset=utf8"))
# Generally ...
# sqlalchemy_url = Literal(
#    "mysql+mysqldb://user:password@hostname:port/database?charset=utf8")


class SQLAMySQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLAMySQLGraphTestCase,self).setUp(
            uri=self.uri, storename=self.storename)

    def tearDown(self):
        super(SQLAMySQLGraphTestCase, self).tearDown(uri=self.uri)


class SQLAMySQLContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLAMySQLContextTestCase, self).setUp(
            uri=self.uri, storename=self.storename)

    def tearDown(self):
        super(SQLAMySQLContextTestCase, self).tearDown(uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")

    
class SQLAMySQLIssueTestCase(unittest.TestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def test_issue_4(self):
        from rdflib.graph import ConjunctiveGraph as Graph
        from rdflib.store import NO_STORE, VALID_STORE
        from rdflib.term import URIRef

        ident = URIRef("rdflib_test")
        g = Graph(store="SQLAlchemy", identifier=ident)
        rt = g.open(self.uri, create=True)
        if rt == NO_STORE:
            g.open(self.uri, create=True)
        else:
            assert rt == VALID_STORE, "The underlying store is corrupt"
        g.destroy(self.uri)


SQLAMySQLGraphTestCase.storetest = True
SQLAMySQLContextTestCase.storetest = True
SQLAMySQLIssueTestCase.storetest = True

if __name__ == '__main__':
    unittest.main()
