import logging
import os
import unittest

import pytest
try:
    import psycopg2  # noqa
    assert psycopg2  # quiets unused import warning
except ImportError:
    pytest.skip("psycopg2 not installed, skipping PgSQL tests",
            allow_module_level=True)

from . import context_case
from . import graph_case


if os.environ.get("DB") != "pgsql":
    pytest.skip("PgSQL not under test", allow_module_level=True)

sqlalchemy_url = os.environ.get(
    "DBURI",
    "postgresql+psycopg2://postgres@localhost/test")

_logger = logging.getLogger(__name__)


class SQLAPgSQLGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True


class SQLAPgSQLContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def testLenInMultipleContexts(self):
        pytest.skip("Known issue.")


SQLAPgSQLGraphTestCase.storetest = True
SQLAPgSQLContextTestCase.storetest = True

if __name__ == "__main__":
    unittest.main()
