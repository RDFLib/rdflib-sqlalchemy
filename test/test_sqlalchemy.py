import os
import logging
import unittest
from nose import SkipTest

from rdflib import (
    ConjunctiveGraph,
    Literal,
    URIRef,
)
from rdflib import plugin
from rdflib.store import Store

import rdflib_sqlalchemy2 


_logger = logging.getLogger(__name__)

michel = URIRef(u"michel")
tarek = URIRef(u"tarek")
bob = URIRef(u"bob")
likes = URIRef(u"likes")
hates = URIRef(u"hates")
pizza = URIRef(u"pizza")
cheese = URIRef(u"cheese")

sqlalchemy_url = os.environ.get(
    "DBURI",
    "postgresql+psycopg2://postgres@localhost/test")


class mock_cursor():
    def execute(x):
        raise Exception("Forced exception")


class SQLATestCase(unittest.TestCase):
    identifier = URIRef("rdflib_test")
    dburi = Literal(sqlalchemy_url)

    def setUp(self):
        rdflib_sqlalchemy2.registerplugins()

        self.store = plugin.get(
            "SQLAlchemy2", Store)(identifier=self.identifier)
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
        rdflib_sqlalchemy2.registerplugins()
        self.assert_(plugin.get("SQLAlchemy2", Store) is not None)
        p = plugin._plugins
        self.assert_(("SQLAlchemy2", Store) in p, p)
        del p[("SQLAlchemy2", Store)]
        plugin._plugins = p
        rdflib_sqlalchemy2.registerplugins()
        self.assert_(("SQLAlchemy2", Store) in p, p)

    def test_namespaces(self):
        self.assert_(list(self.graph.namespaces()) != [])

    def test_contexts_without_triple(self):
        self.assert_(list(self.graph.contexts()) == [])

    def test_contexts_with_triple(self):
        statemnt = (michel, likes, pizza)
        self.assert_(self.graph.contexts(triple=statemnt) != [])

    def test__len(self):
        raise SkipTest("sqlite only test? len is: " + str(self.store.__len__()))
        self.assert_(self.store.__len__() == 0)

    def test__remove_context(self):
        self.store._remove_context(self.identifier)


if __name__ == "__main__":
    unittest.main()
