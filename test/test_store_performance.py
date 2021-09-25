from __future__ import print_function
import unittest
import gc
import os
import shutil
from time import time
from tempfile import mkdtemp

from rdflib import Graph
from six.moves.urllib.request import pathname2url

try:
    from rdflib.plugins.stores.memory import Memory
except ImportError:
    # rdflib<6.0.0
    from rdflib.plugins.memory import IOMemory as Memory


class StoreTestCase(unittest.TestCase):

    """
    Test case for testing store performance... probably should be
    something other than a unit test... but for now we'll add it as a
    unit test.
    """
    store = None
    path = None
    storetest = True
    performancetest = True

    def setUp(self):
        self.gcold = gc.isenabled()
        gc.collect()
        gc.disable()
        if self.store is None:
            store = Memory()
        else:
            store = self.store
        self.graph = Graph(store=store)
        self.tempdir = None
        if not self.path:
            self.tempdir = mkdtemp()
            path = pathname2url(self.tempdir)
        else:
            path = self.path
        self.path = path
        self.graph.open(self.path, create=True)
        self.input = Graph()

    def tearDown(self):
        self.graph.close()
        if self.gcold:
            gc.enable()
        self.graph.close()
        del self.graph
        tempdir = getattr(self, 'tempdir', None)
        if tempdir is not None:
            shutil.rmtree(tempdir)

    def testTime(self):
        # number = 1
        print('"Load %s": [' % self.store)
        for i in ['500triples', '1ktriples', '2ktriples',
                  '3ktriples', '5ktriples', '10ktriples',
                  '25ktriples']:
            inputloc = os.getcwd() + '/test/sp2b/%s.n3' % i
            # cleanup graph's so that BNodes in input data
            # won't create random results
            self.input = Graph()
            self.graph.remove((None, None, None))
            res = self._testInput(inputloc)
            print("Loaded %5d triples in %ss" % (len(self.graph), res.strip()))
        print("],")
        print('"Read %s": [' % self.store)
        t0 = time()
        for _i in self.graph.triples((None, None, None)):
            pass
        self.assertEqual(len(self.graph), 25161)
        t1 = time()
        print("%.3gs" % (t1 - t0))
        print("],")
        print('"Delete %s": [' % self.store)
        t0 = time()
        self.graph.remove((None, None, None))
        self.assertEqual(len(self.graph), 0)
        t1 = time()
        print("%.3g " % (t1 - t0))
        print("],")

    def _testInput(self, inputloc):
        store = self.graph
        self.input.parse(location=inputloc, format="n3")

        t0 = time()
        store.addN(tuple(t) + (store,) for t in self.input)
        t1 = time()
        return "%.3g " % (t1 - t0)


class SQLAlchemyStoreTestCase(StoreTestCase):
    """SQLAlchemy Store."""

    store = "SQLAlchemy"

    def setUp(self):
        """Setup."""
        self.store = "SQLAlchemy"
        self.path = "sqlite:///%(here)s/test/tmpdb.sqlite" % {
            "here": os.getcwd()
        }
        StoreTestCase.setUp(self)


if __name__ == '__main__':
    unittest.main()
