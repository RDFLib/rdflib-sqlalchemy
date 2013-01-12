from rdflib import Graph, BNode, Literal, URIRef, RDFS, RDF, plugin
from rdflib.store import Store
import os


# Work in progress


def investigate_len_issue():
    store = plugin.get('SQLAlchemy', Store)(
        identifier=URIRef("rdflib_test"),
        configuration=Literal("sqlite:///%(here)s/development.sqlite" % {
                                                        "here": os.getcwd()}))
    g = Graph(store)
    print(len(g))
    statementId = BNode()
    g.add((statementId, RDF.type, RDF.Statement))
    g.add((statementId, RDF.subject,
           URIRef(u'http://rdflib.net/store/ConjunctiveGraph')))
    g.add((statementId, RDF.predicate, RDFS.label))
    g.add((statementId, RDF.object, Literal("Conjunctive Graph")))
    print(len(g))
    for s, p, o in g:
        print(type(s))

    for s, p, o in g.triples((None, RDF.object, None)):
        print(o)

    g.remove((statementId, RDF.type, RDF.Statement))
    print(len(g))
    os.unlink("%(here)s/development.sqlite" % {"here": os.getcwd()})

if __name__ == '__main__':
    investigate_len_issue()
