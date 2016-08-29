"""Store performance tests."""
import gc
import itertools
import os
import unittest
from random import random
from tempfile import mkdtemp
from time import time

from nose import SkipTest
from rdflib import Graph
from rdflib import URIRef
from six.moves.urllib.request import pathname2url


raise SkipTest("Store performance test suspended")


def random_uri():
    """Return random URI."""
    return URIRef("%s" % random())


class StoreTestCase(unittest.TestCase):
    """
    Test case for testing store performance.

    Probably should be something other than a unit test
    but for now we"ll add it as a unit test.
    """

    store = "IOMemory"
    path = None
    storetest = True
    performancetest = True

    def setUp(self):
        """Setup."""
        self.gcold = gc.isenabled()
        gc.collect()
        gc.disable()
        self.graph = Graph(store=self.store)
        if self.path is None:
            self.path = pathname2url(mkdtemp())
        self.graph.open(self.path)
        self.input = Graph()

    def tearDown(self):
        """Teardown."""
        self.graph.close()
        if self.gcold:
            gc.enable()
        # TODO: delete a_tmp_dir
        self.graph.close()
        del self.graph
        if hasattr(self, "path") and self.path is not None:
            if os.path.exists(self.path):
                if os.path.isdir(self.path):
                    for f in os.listdir(self.path):
                        os.unlink(self.path + "/" + f)
                    os.rmdir(self.path)
                elif len(self.path.split(":")) == 1:
                    os.unlink(self.path)
                else:
                    os.remove(self.path)

    def testTime(self):
        """Test timing."""
        # number = 1
        print("'%s': [" % self.store)
        for i in ["500triples", "1ktriples", "2ktriples",
                  "3ktriples", "5ktriples", "10ktriples",
                  "25ktriples"]:
            inputloc = os.getcwd() + "/test/sp2b/%s.n3" % i
            res = self._testInput(inputloc)
            print("%s," % res.strip())
        print("],")

    def _testInput(self, inputloc):
        number = 1
        store = self.graph
        self.input.parse(location=inputloc, format="n3")

        def add_from_input():
            for t in self.input:
                store.add(t)
        it = itertools.repeat(None, number)
        t0 = time()
        for _i in it:
            add_from_input()
        t1 = time()
        return "%.3g " % (t1 - t0)


class SQLAlchemyStoreTestCase(StoreTestCase):
    """SQLAlchemy Store."""

    store = "SQLAlchemy"

    def setUp(self):
        """Setup."""
        self.store = "SQLAlchemy"
        self.path = "sqlite:///%(here)s/test/tmpdb.sqlite" % {
            "here": os.getcwd()}
        StoreTestCase.setUp(self)

if __name__ == "__main__":
    unittest.main()
