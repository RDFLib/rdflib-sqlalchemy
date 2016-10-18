import logging
import unittest

from rdflib import (
    BNode,
    ConjunctiveGraph,
    Literal,
    URIRef,
)
from rdflib import plugin
from rdflib.store import Store

from rdflib_sqlalchemy import registerplugins
from rdflib_sqlalchemy.store import (
    skolemise,
    deskolemise,
)


_logger = logging.getLogger(__name__)

michel = URIRef(u"michel")
tarek = URIRef(u"tarek")
bob = URIRef(u"bob")
likes = URIRef(u"likes")
hates = URIRef(u"hates")
pizza = URIRef(u"pizza")
cheese = URIRef(u"cheese")


class mock_cursor():
    def execute(x):
        raise Exception("Forced exception")


class SQLATestCase(unittest.TestCase):
    identifier = URIRef("rdflib_test")
    dburi = Literal("sqlite://")

    def setUp(self):
        self.store = plugin.get(
            "SQLAlchemy", Store)(identifier=self.identifier)
        self.graph = ConjunctiveGraph(self.store, identifier=self.identifier)
        self.graph.open(self.dburi, create=True)

    def tearDown(self):
        self.graph.destroy(self.dburi)
        try:
            self.graph.close()
        except:
            pass

    def test_registerplugins(self):
        # I doubt this is quite right for a fresh pip installation,
        # this test is mainly here to fill a coverage gap.
        registerplugins()
        self.assert_(plugin.get("SQLAlchemy", Store) is not None)
        p = plugin._plugins
        self.assert_(("SQLAlchemy", Store) in p, p)
        del p[("SQLAlchemy", Store)]
        plugin._plugins = p
        registerplugins()
        self.assert_(("SQLAlchemy", Store) in p, p)

    def test_skolemisation(self):
        testbnode = BNode()
        statemnt = (michel, likes, testbnode)
        res = skolemise(statemnt)
        self.assert_("bnode:N" in str(res[2]), res)

    def test_deskolemisation(self):
        testbnode = BNode()
        statemnt = (michel, likes, testbnode)
        res = deskolemise(statemnt)
        self.assert_(str(res[2]).startswith("N"), res)

    def test_redeskolemisation(self):
        testbnode = BNode()
        statemnt = skolemise((michel, likes, testbnode))
        res = deskolemise(statemnt)
        self.assert_(str(res[2]).startswith("N"), res)

    def test_namespaces(self):
        self.assert_(list(self.graph.namespaces()) != [])

    def test_contexts_without_triple(self):
        self.assert_(list(self.graph.contexts()) == [])

    def test_contexts_with_triple(self):
        statemnt = (michel, likes, pizza)
        self.assert_(self.graph.contexts(triple=statemnt) != [])

    def test__len(self):
        self.assert_(self.store.__len__() == 0)

    def test__remove_context(self):
        self.store._remove_context(self.identifier)


if __name__ == "__main__":
    unittest.main()
