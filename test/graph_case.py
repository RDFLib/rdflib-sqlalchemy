import unittest
from rdflib import Graph
from rdflib import RDF
from rdflib import URIRef

class GraphTestCase(unittest.TestCase):
    storetest = True
    identifier = URIRef("rdflib_test")

    michel = URIRef(u'michel')
    tarek = URIRef(u'tarek')
    bob = URIRef(u'bob')
    likes = URIRef(u'likes')
    hates = URIRef(u'hates')
    pizza = URIRef(u'pizza')
    cheese = URIRef(u'cheese')
    
    def setUp(self, uri='sqlite://', storename=None):
        self.graph = Graph(storename, identifier=self.identifier)
        self.graph.destroy(self.uri)
        self.graph.open(uri, create=True)
    
    def tearDown(self, uri='sqlite://'):
        self.graph.destroy(uri)
        try:
            self.graph.close()
        except:
            pass
    
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
        self.graph.add((bob, hates, michel)) # gasp!
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
        self.graph.remove((bob, hates, michel)) # gasp!
    
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
        asserte = self.assertEquals
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
    
    def testStatementNode(self):
        graph = self.graph
        
        from rdflib.term import Statement
        c = URIRef("http://example.org/foo#c")
        r = URIRef("http://example.org/foo#r")
        s = Statement((self.michel, self.likes, self.pizza), c)
        graph.store.add((s, RDF.value, r), quoted=True)
        self.assertEquals(r, graph.value(s, RDF.value))
        self.assertEquals(s, graph.value(predicate=RDF.value, object=r))
    
    def testGraphValue(self):
        from rdflib.graph import GraphValue
        
        graph = self.graph
        
        alice = URIRef("alice")
        bob = URIRef("bob")
        pizza = URIRef("pizza")
        cheese = URIRef("cheese")
        
        g1 = Graph()
        g1.add((alice, RDF.value, pizza))
        g1.add((bob, RDF.value, cheese))
        g1.add((bob, RDF.value, pizza))
        
        g2 = Graph()
        g2.add((bob, RDF.value, pizza))
        g2.add((bob, RDF.value, cheese))
        g2.add((alice, RDF.value, pizza))
        
        gv1 = GraphValue(store=graph.store, graph=g1)
        gv2 = GraphValue(store=graph.store, graph=g2)
        graph.add((gv1, RDF.value, gv2))
        v = graph.value(gv1)
        self.assertEquals(gv2, v)
        #print list(gv2)
        #print gv2.identifier
        graph.remove((gv1, RDF.value, gv2))
    
    def testConnected(self):
        graph = self.graph
        self.addStuff()
        self.assertEquals(True, graph.connected())
        
        jeroen = URIRef("jeroen")
        unconnected = URIRef("unconnected")
        
        graph.add((jeroen,self.likes,unconnected))
        
        self.assertEquals(False, graph.connected())
    
    def testSub(self):
        g1=Graph()
        g2=Graph()
        
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
        
        g1.add((tarek, likes, pizza))
        g1.add((bob, likes, cheese))
        
        g2.add((bob, likes, cheese))
        
        g3=g1-g2
        
        self.assertEquals(len(g3), 1)
        self.assertEquals((tarek, likes, pizza) in g3, True)
        self.assertEquals((tarek, likes, cheese) in g3, False)
        
        self.assertEquals((bob, likes, cheese) in g3, False)
        
        g1-=g2
        
        self.assertEquals(len(g1), 1)
        self.assertEquals((tarek, likes, pizza) in g1, True)
        self.assertEquals((tarek, likes, cheese) in g1, False)
        
        self.assertEquals((bob, likes, cheese) in g1, False)
    
    def testGraphAdd(self):
        g1=Graph()
        g2=Graph()
        
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
        
        g1.add((tarek, likes, pizza))
        
        g2.add((bob, likes, cheese))
        
        g3=g1+g2
        
        self.assertEquals(len(g3), 2)
        self.assertEquals((tarek, likes, pizza) in g3, True)
        self.assertEquals((tarek, likes, cheese) in g3, False)
        
        self.assertEquals((bob, likes, cheese) in g3, True)
        
        g1+=g2
        
        self.assertEquals(len(g1), 2)
        self.assertEquals((tarek, likes, pizza) in g1, True)
        self.assertEquals((tarek, likes, cheese) in g1, False)
        
        self.assertEquals((bob, likes, cheese) in g1, True)
    
    def testGraphIntersection(self):
        g1=Graph()
        g2=Graph()
        
        tarek = self.tarek
        michel = self.michel
        bob = self.bob
        likes = self.likes
        hates = self.hates
        pizza = self.pizza
        cheese = self.cheese
        
        g1.add((tarek, likes, pizza))
        g1.add((michel, likes, cheese))
        
        g2.add((bob, likes, cheese))
        g2.add((michel, likes, cheese))
        
        g3=g1*g2
        
        self.assertEquals(len(g3), 1)
        self.assertEquals((tarek, likes, pizza) in g3, False)
        self.assertEquals((tarek, likes, cheese) in g3, False)
        
        self.assertEquals((bob, likes, cheese) in g3, False)
        
        self.assertEquals((michel, likes, cheese) in g3, True)
        
        g1*=g2
        
        self.assertEquals(len(g1), 1)
        
        self.assertEquals((tarek, likes, pizza) in g1, False)
        self.assertEquals((tarek, likes, cheese) in g1, False)
        
        self.assertEquals((bob, likes, cheese) in g1, False)
        
        self.assertEquals((michel, likes, cheese) in g1, True)



xmltestdoc="""<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns="http://example.org/"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
>
  <rdf:Description rdf:about="http://example.org/a">
    <b rdf:resource="http://example.org/c"/>
  </rdf:Description>
</rdf:RDF>
"""

n3testdoc="""@prefix : <http://example.org/> .

:a :b :c .
"""

nttestdoc="<http://example.org/a> <http://example.org/b> <http://example.org/c> .\n"

if __name__ == '__main__':
    unittest.main()
