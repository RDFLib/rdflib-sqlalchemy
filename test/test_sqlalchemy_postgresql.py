from nose.exc import SkipTest
try:
    import psycopg2
except ImportError:
    raise SkipTest("psycopg2 not install, skipping PostgreSQL tests")
import logging
_logger = logging.getLogger(__name__)
import context_case
import graph_case

sqlalchemy_url = 'postgresql+psycopg2://gjh:50uthf0rk@localhost/test'

class SQLAlchemyPostgreSQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    def setUp(self):
        graph_case.GraphTestCase.setUp(self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        self.create = False
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)

    def testStatementNode(self):
        raise SkipTest("Known issue.")

class SQLAlchemyPostgreSQLContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    def setUp(self):
        context_case.ContextTestCase.setUp(self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        self.create = False
        context_case.ContextTestCase.tearDown(self, uri=self.uri)

    def testConjunction(self):
        raise SkipTest("Known issue.")

    def testContexts(self):
        raise SkipTest("Known issue.")

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")

SQLAlchemyPostgreSQLGraphTestCase.storetest = True
SQLAlchemyPostgreSQLContextTestCase.storetest = True
