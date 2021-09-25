# -*- coding: utf-8 -*-
import unittest

from rdflib import Graph, URIRef, Literal, plugin, RDF
from rdflib.parser import StringInputSource
from six import PY3
from rdflib.store import Store


class GraphTestCase(unittest.TestCase):
    storetest = True
    identifier = URIRef("rdflib_test")

    michel = URIRef(u"michel")
    tarek = URIRef(u"tarek")
    bob = URIRef(u"bob")
    likes = URIRef(u"likes")
    hates = URIRef(u"hates")
    pizza = URIRef(u"pizza")
    cheese = URIRef(u"cheese")

    namespace_dc = "http://purl.org/dc/elements/1.1/"
    namespace_dct = "http://purl.org/dc/terms/"
    namespace_saws = "http://purl.org/saws/ontology#"

    def setUp(self, uri="sqlite://", storename=None):
        store = plugin.get(storename, Store)(identifier=self.identifier)
        self.graph = Graph(store, identifier=self.identifier)
        self.graph.open(uri, create=True)

    def tearDown(self, uri="sqlite://"):
        self.graph.destroy(uri)
        self.graph.close()

    def addStuff(self):
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese

        self.graph.add((tarek, likes, pizza))
        self.graph.add((tarek, likes, cheese))
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.add((bob, likes, cheese))
        self.graph.add((bob, hates, pizza))
        self.graph.add((bob, hates, michel))  # gasp!
        self.graph.commit()

    def removeStuff(self):
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese

        self.graph.remove((tarek, likes, pizza))
        self.graph.remove((tarek, likes, cheese))
        self.graph.remove((michel, likes, pizza))
        self.graph.remove((michel, likes, cheese))
        self.graph.remove((bob, likes, cheese))
        self.graph.remove((bob, hates, pizza))
        self.graph.remove((bob, hates, michel))  # gasp!

    def testAdd(self):
        self.addStuff()

    def testRemove(self):
        self.addStuff()
        self.removeStuff()

    def testTriples(self):
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
        asserte = self.assertEqual
        triples = self.graph.triples
        Any = None

        self.addStuff()

        # unbound subjects
        asserte(len(list(triples((Any, likes, pizza)))), 2)
        asserte(len(list(triples((Any, hates, pizza)))), 1)
        asserte(len(list(triples((Any, likes, cheese)))), 3)
        asserte(len(list(triples((Any, hates, cheese)))), 0)

        # unbound objects
        asserte(len(list(triples((michel, likes, Any)))), 2)
        asserte(len(list(triples((tarek, likes, Any)))), 2)
        asserte(len(list(triples((bob, hates, Any)))), 2)
        asserte(len(list(triples((bob, likes, Any)))), 1)

        # unbound predicates
        asserte(len(list(triples((michel, Any, cheese)))), 1)
        asserte(len(list(triples((tarek, Any, cheese)))), 1)
        asserte(len(list(triples((bob, Any, pizza)))), 1)
        asserte(len(list(triples((bob, Any, michel)))), 1)

        # unbound subject, objects
        asserte(len(list(triples((Any, hates, Any)))), 2)
        asserte(len(list(triples((Any, likes, Any)))), 5)

        # unbound predicates, objects
        asserte(len(list(triples((michel, Any, Any)))), 2)
        asserte(len(list(triples((bob, Any, Any)))), 3)
        asserte(len(list(triples((tarek, Any, Any)))), 2)

        # unbound subjects, predicates
        asserte(len(list(triples((Any, Any, pizza)))), 3)
        asserte(len(list(triples((Any, Any, cheese)))), 3)
        asserte(len(list(triples((Any, Any, michel)))), 1)

        # all unbound
        asserte(len(list(triples((Any, Any, Any)))), 7)
        self.removeStuff()
        asserte(len(list(triples((Any, Any, Any)))), 0)

    def testConnected(self):
        graph = self.graph
        self.addStuff()
        self.assertEqual(True, graph.connected())

        jeroen = URIRef("jeroen")
        unconnected = URIRef("unconnected")

        graph.add((jeroen, self.likes, unconnected))

        self.assertEqual(False, graph.connected())

    def testSub(self):
        g1 = Graph()
        g2 = Graph()

        tarek = self.tarek
        # michel = self.michel
        bob = self.bob
        likes = self.likes
        # hates = self.hates
        pizza = self.pizza
        cheese = self.cheese

        g1.add((tarek, likes, pizza))
        g1.add((bob, likes, cheese))

        g2.add((bob, likes, cheese))

        g3 = g1 - g2

        self.assertEqual(len(g3), 1)
        self.assertEqual((tarek, likes, pizza) in g3, True)
        self.assertEqual((tarek, likes, cheese) in g3, False)

        self.assertEqual((bob, likes, cheese) in g3, False)

        g1 -= g2

        self.assertEqual(len(g1), 1)
        self.assertEqual((tarek, likes, pizza) in g1, True)
        self.assertEqual((tarek, likes, cheese) in g1, False)

        self.assertEqual((bob, likes, cheese) in g1, False)

    def testGraphAdd(self):
        g1 = Graph()
        g2 = Graph()

        tarek = self.tarek
        # michel = self.michel
        bob = self.bob
        likes = self.likes
        # hates = self.hates
        pizza = self.pizza
        cheese = self.cheese

        g1.add((tarek, likes, pizza))

        g2.add((bob, likes, cheese))

        g3 = g1 + g2

        self.assertEqual(len(g3), 2)
        self.assertEqual((tarek, likes, pizza) in g3, True)
        self.assertEqual((tarek, likes, cheese) in g3, False)

        self.assertEqual((bob, likes, cheese) in g3, True)

        g1 += g2

        self.assertEqual(len(g1), 2)
        self.assertEqual((tarek, likes, pizza) in g1, True)
        self.assertEqual((tarek, likes, cheese) in g1, False)

        self.assertEqual((bob, likes, cheese) in g1, True)

    def testGraphIntersection(self):
        g1 = Graph()
        g2 = Graph()

        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        # hates = self.hates
        pizza = self.pizza
        cheese = self.cheese

        g1.add((tarek, likes, pizza))
        g1.add((michel, likes, cheese))

        g2.add((bob, likes, cheese))
        g2.add((michel, likes, cheese))

        g3 = g1 * g2

        self.assertEqual(len(g3), 1)
        self.assertEqual((tarek, likes, pizza) in g3, False)
        self.assertEqual((tarek, likes, cheese) in g3, False)

        self.assertEqual((bob, likes, cheese) in g3, False)

        self.assertEqual((michel, likes, cheese) in g3, True)

        g1 *= g2

        self.assertEqual(len(g1), 1)

        self.assertEqual((tarek, likes, pizza) in g1, False)
        self.assertEqual((tarek, likes, cheese) in g1, False)

        self.assertEqual((bob, likes, cheese) in g1, False)

        self.assertEqual((michel, likes, cheese) in g1, True)

    def testStoreLiterals(self):
        bob = self.bob
        says = URIRef(u"says")
        hello = Literal(u"hello", lang="en")
        konichiwa = Literal(u"こんにちは", lang="ja")
        something = Literal(u"something")

        self.graph.add((bob, says, hello))
        self.graph.add((bob, says, konichiwa))
        self.graph.add((bob, says, something))
        self.graph.commit()

        objs = list(self.graph.objects(subject=bob, predicate=says))
        for o in objs:
            if o.value == u"hello":
                self.assertEqual(o.language, "en")
            elif o.value == u"こんにちは":
                self.assertEqual(o.language, "ja")
            elif o.value == u"something":
                self.assertEqual(o.language, None)
            else:
                self.fail()
        self.assertEqual(len(list(objs)), 3)

    def testStoreLiteralsXml(self):
        bob = self.bob
        says = URIRef(u"http://www.rdflib.net/terms/says")
        objects = [
            Literal(u"I'm the one", lang="en"),
            Literal(u"こんにちは", lang="ja"),
            Literal(u"les garçons à Noël reçoivent des œufs", lang="fr")]

        testdoc = (PY3 and bytes(xmltestdocXml, "UTF-8")) or xmltestdocXml

        self.graph.parse(StringInputSource(testdoc), format="xml")

        objs = list(self.graph)
        self.assertEqual(len(objs), 3)
        for o in objs:
            self.assertEqual(o[0], bob)
            self.assertEqual(o[1], says)
            self.assertIn(o[2], objects)

    def testStoreLiteralsXmlQuote(self):
        bob = self.bob
        says = URIRef(u"http://www.rdflib.net/terms/says")
        imtheone = Literal(u"I'm the one", lang="en")

        testdoc = (PY3 and bytes(xmltestdocXmlQuote, "UTF-8")) or xmltestdocXmlQuote

        self.graph.parse(StringInputSource(testdoc), format="xml")

        objs = list(self.graph)
        self.assertEqual(len(objs), 1)
        o = objs[0]
        self.assertEqual(o, (bob, says, imtheone))

    def testBindNamespace(self):
        """ Check that bound namespaced with prefix (including empty ones) are correctly kept """
        self.graph.bind("", self.namespace_dc)
        self.graph.bind("dct", self.namespace_dct)
        self.assertEqual(
            self.graph.qname(self.namespace_dct + u"title"), "dct:title",
            "Prefixed namespace should be stored and retrieved"
        )
        self.assertEqual(
            self.graph.qname(self.namespace_dc + u"title"), "title",
            "Empty prefixes for namespace should be stored and retrieved"
        )
        self.assertEqual(
            self.graph.qname(self.namespace_saws + u"title"), "ns1:title",
            "Unknown prefixes for namespace should be transformed to nsX"
        )

    def testTriplesChoices(self):
        likes = self.likes
        pizza = self.pizza
        cheese = self.cheese
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        self.addStuff()
        trips = self.graph.triples_choices((None, likes, [pizza, cheese]))
        self.assertEqual(
            set(trips),
            set([(tarek, likes, pizza),
                 (tarek, likes, pizza),
                 (tarek, likes, cheese),
                 (michel, likes, pizza),
                 (michel, likes, cheese),
                 (bob, likes, cheese)])
        )

    def test_type_add(self):
        trip = (URIRef('http://example.org#type-add'), RDF.type, URIRef('http://example.org/cra'))
        self.graph.add(trip)
        self.graph.add(trip)
        # No exception raised

    def test_type_addn(self):
        quad = (URIRef('http://example.org#type-addn'), RDF.type, URIRef('http://example.org/cra'), self.graph)
        self.graph.addN([quad, quad])
        # No exception raised

    def test_add(self):
        trip = (URIRef('http://example.org#add'), URIRef('http://example.org/blah'), URIRef('http://example.org/cra'))
        self.graph.add(trip)
        self.graph.add(trip)
        # No exception raised

    def test_addn(self):
        quad = (URIRef('http://example.org#addn'),
                URIRef('http://example.org/blah'),
                URIRef('http://example.org/cra'),
                self.graph)
        self.graph.addN([quad, quad])
        # No exception raised

    def test_namespace_change_prefix_binding(self):
        nm = self.graph.namespace_manager
        nm.bind('change_binding', URIRef('http://example.org/change-binding-1#'),
                replace=True)
        nm.bind('change_binding', URIRef('http://example.org/change-binding-2#'),
                replace=True)
        self.assertIn(
            ('change_binding', URIRef('http://example.org/change-binding-2#')),
            list(nm.namespaces())
        )

    def test_namespace_rebind_prefix(self):
        nm = self.graph.namespace_manager
        nm.bind('rebind', URIRef('http://example.org/rebind#'))
        nm.bind('rebind', URIRef('http://example.org/rebind#'))

    def test_add_duplicate_length(self):
        '''
        Test that adding duplicate triples doesn't increase the length of the graph
        '''
        trip = (URIRef('http://example.org#add'), URIRef('http://example.org/blah'), URIRef('http://example.org/cra'))
        self.graph.add(trip)
        self.graph.add(trip)
        self.assertEqual(len(self.graph), 1)
    # additional tests
    # - add "duplicate" triples and query -- ensure the graph length counts only distinct
    #   triples
    # - add duplicate triples and query -- ensure there are no duplicate entries in the
    #   query result
    # - repeat the above two for type triples
    # - test with quoted graphs (not even sure how that works)


xmltestdoc = """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns="http://example.org/"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
>
  <rdf:Description rdf:about="http://example.org/a">
    <b rdf:resource="http://example.org/c"/>
  </rdf:Description>
</rdf:RDF>
"""

xmltestdocXml = """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns:ns1="http://www.rdflib.net/terms/"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
>
  <rdf:Description rdf:about="bob">
    <ns1:says xml:lang="en">I'm the one</ns1:says>
  </rdf:Description>
  <rdf:Description rdf:about="bob">
    <ns1:says xml:lang="ja">こんにちは</ns1:says>
  </rdf:Description>
  <rdf:Description rdf:about="bob">
    <ns1:says xml:lang="fr">les garçons à Noël reçoivent des œufs</ns1:says>
  </rdf:Description>
</rdf:RDF>
"""

xmltestdocXmlQuote = """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns:ns1="http://www.rdflib.net/terms/"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
>
  <rdf:Description rdf:about="bob">
    <ns1:says xml:lang="en">I'm the one</ns1:says>
  </rdf:Description>
</rdf:RDF>
"""


n3testdoc = """@prefix : <http://example.org/> .

:a :b :c .
"""

nttestdoc = "<http://example.org/a> <http://example.org/b> <http://example.org/c> .\n"

if __name__ == "__main__":
    unittest.main()
