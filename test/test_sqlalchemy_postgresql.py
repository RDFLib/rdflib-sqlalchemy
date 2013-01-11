import os
import unittest
from nose.exc import SkipTest
if os.environ.get('DB') != 'pgsql':
    raise SkipTest("PgSQL not under test")
try:
    import psycopg2
except ImportError:
    raise SkipTest("psycopg2 not install, skipping PgSQL tests")
import logging
_logger = logging.getLogger(__name__)
import context_case
import graph_case

import sys
if '.virtualenvs/rdflib/' in sys.executable:
    sqlalchemy_url = os.environ['DBURI']
else:
    sqlalchemy_url = \
        'PgSQL+psycopg2://postgres@localhost/rdflibsqla_test'


class SQLAPgSQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        graph_case.GraphTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)

    def testStatementNode(self):
        raise SkipTest("Known issue.")


class SQLAPgSQLContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        context_case.ContextTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        context_case.ContextTestCase.tearDown(self, uri=self.uri)

    def testLenInMultipleContexts(self):
        raise SkipTest("Known issue.")

SQLAPgSQLGraphTestCase.storetest = True
SQLAPgSQLContextTestCase.storetest = True

if __name__ == '__main__':
    unittest.main()
