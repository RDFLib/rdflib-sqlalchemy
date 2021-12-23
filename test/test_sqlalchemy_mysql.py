import logging
import os
import unittest

import pytest
from rdflib import Literal
from rdflib.graph import ConjunctiveGraph as Graph
from rdflib.store import NO_STORE, VALID_STORE
from rdflib.term import URIRef

from . import context_case
from . import graph_case


try:
    import MySQLdb
    assert MySQLdb
    dialect = "mysqldb"
except ImportError:
    pytest.skip("MySQLdb not found, skipping MySQL tests",
            allow_module_level=True)


if os.environ.get("DB") != "mysql":
    pytest.skip("MySQL not under test",
            allow_module_level=True)

_logger = logging.getLogger(__name__)


sqlalchemy_url = Literal(os.environ.get(
    "DBURI",
    "mysql+%s://root@127.0.0.1:3306/test?charset=utf8" % dialect))


class SQLAMySQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLAMySQLGraphTestCase, self).setUp(uri=self.uri, storename=self.storename)

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
        pytest.skip("Known issue.")


class SQLAMySQLIssueTestCase(unittest.TestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url

    def test_issue_4(self):
        ident = URIRef("rdflib_test")
        g = Graph(store="SQLAlchemy", identifier=ident)
        rt = g.open(self.uri, create=True)
        if rt == NO_STORE:
            g.open(self.uri, create=True)
        else:
            assert rt == VALID_STORE, "The underlying store is not valid: State: %s" % rt
        g.destroy(self.uri)


if __name__ == "__main__":
    unittest.main()
