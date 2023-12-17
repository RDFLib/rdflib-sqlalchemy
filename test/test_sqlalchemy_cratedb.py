import logging
import os
import unittest

import pytest
try:
    import crate  # noqa
    assert crate  # quiets unused import warning
except ImportError:
    pytest.skip("crate not installed, skipping CrateDB tests",
                allow_module_level=True)

from . import context_case
from . import graph_case


if os.environ.get("DB") != "crate":
    pytest.skip("CrateDB not under test", allow_module_level=True)

sqlalchemy_url = os.environ.get(
    "DBURI",
    "crate://crate@localhost/")

_logger = logging.getLogger(__name__)


class SQLACrateDBGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLACrateDBGraphTestCase, self).setUp(
            uri=self.uri,
            storename=self.storename,
        )

    def tearDown(self):
        super(SQLACrateDBGraphTestCase, self).tearDown(uri=self.uri)


class SQLACrateDBContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemy"
    uri = sqlalchemy_url
    create = True

    def setUp(self):
        super(SQLACrateDBContextTestCase, self).setUp(
            uri=self.uri,
            storename=self.storename,
        )

    def tearDown(self):
        super(SQLACrateDBContextTestCase, self).tearDown(uri=self.uri)

    def testLenInMultipleContexts(self):
        pytest.skip("Known issue.")


SQLACrateDBGraphTestCase.storetest = True
SQLACrateDBContextTestCase.storetest = True

if __name__ == "__main__":
    unittest.main()
