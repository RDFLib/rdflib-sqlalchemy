"""pg8000 test."""
import logging
import os
import unittest

import pytest
try:
    import pg8000
    assert pg8000 is not None
except ImportError:
    pytest.skip("pg8000 not installed, skipping PgSQL tests",
            allow_module_level=True)

from . import context_case
from . import graph_case


if os.environ.get("DB") != "pgsql":
    pytest.skip("PgSQL not under test", allow_module_level=True)

_logger = logging.getLogger(__name__)

sqlalchemy_url = os.environ.get(
    "DBURI",
    "postgresql+pg8000://postgres@localhost/test")


class SQLAPgSQLGraphTestCase(graph_case.GraphTestCase):
    """Graph test case."""

    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        """Setup."""
        super(SQLAPgSQLGraphTestCase, self).setUp(
            uri=self.uri, storename=self.storename)

    def tearDown(self):
        """Teardown."""
        super(SQLAPgSQLGraphTestCase, self).tearDown(uri=self.uri)


class SQLAPgSQLContextTestCase(context_case.ContextTestCase):
    """Context test case."""

    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        """Setup."""
        super(SQLAPgSQLContextTestCase, self).setUp(
            uri=self.uri, storename=self.storename)

    def tearDown(self):
        """Teardown."""
        super(SQLAPgSQLContextTestCase, self).tearDown(uri=self.uri)

    def testLenInMultipleContexts(self):
        """Test lin in multiple contexts, known issue."""
        pytest.skip("Known issue.")

# SQLAPgSQLGraphTestCase.storetest = True
# SQLAPgSQLContextTestCase.storetest = True


if __name__ == "__main__":
    unittest.main()
