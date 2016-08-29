import os
import shutil
try:
    from commands import getoutput
except ImportError:
    # Python 3
    from subprocess import getoutput

from rdflib import (
    BNode,
    Graph,
    Literal,
    RDF,
    RDFS,
    URIRef,
    plugin,
)
from rdflib.store import Store


# Work in progress


def investigate_len_issue():
    store = plugin.get("SQLAlchemy", Store)(
        identifier=URIRef("rdflib_test"),
        configuration=Literal("sqlite:///%(here)s/development.sqlite" % {
                                                        "here": os.getcwd()}))
    g0 = Graph("Sleepycat")
    g0.open("/tmp/foo", create=True)
    print("Len g0 on opening: %s\n" % len(g0))
    g1 = Graph(store)
    print("Len g1 on opening: %s\n" % len(g1))
    statementId = BNode()
    print("Adding %s\n\t%s\n\t%s\n" % (statementId, RDF.type, RDF.Statement))
    g0.add((statementId, RDF.type, RDF.Statement))
    g1.add((statementId, RDF.type, RDF.Statement))
    print("Adding %s\n\t%s\n\t%s\n" % (statementId, RDF.subject,
          URIRef(u"http://rdflib.net/store/ConjunctiveGraph")))
    g0.add((statementId, RDF.subject,
           URIRef(u"http://rdflib.net/store/ConjunctiveGraph")))
    g1.add((statementId, RDF.subject,
           URIRef(u"http://rdflib.net/store/ConjunctiveGraph")))
    print("Adding %s\n\t%s\n\t%s\n" % (statementId, RDF.predicate, RDFS.label))
    g0.add((statementId, RDF.predicate, RDFS.label))
    g1.add((statementId, RDF.predicate, RDFS.label))
    print("Adding %s\n\t%s\n\t%s\n" % (
        statementId, RDF.object, Literal("Conjunctive Graph")))
    g0.add((statementId, RDF.object, Literal("Conjunctive Graph")))
    print("Len g0 after adding 4 triples %s\n" % len(g0))
    g1.add((statementId, RDF.object, Literal("Conjunctive Graph")))
    print("Len g1 after adding 4 triples %s\n" % len(g1))
    print(g0.serialize(format="nt") + "\n")
    for s, p, o in g0:
        print("s = %s\n\tp = %s\n\to = %s\n" % (
            repr(s), repr(p), repr(o)))
    print(g1.serialize(format="nt") + "\n")
    for s, p, o in g1:
        print("s = %s\n\tp = %s\n\to = %s\n" % (
            repr(s), repr(p), repr(o)))
    getoutput("cp development.sqlite devcopy.sqlite")
    print("Removing %s\n\t%s\n\t%s\n" % (statementId, RDF.type, RDF.Statement))
    g0.remove((statementId, RDF.type, RDF.Statement))
    print("Len g0 after removal %s\n" % len(g0))
    g1.remove((statementId, RDF.type, RDF.Statement))
    print("Len g1 after removal %s\n" % len(g1))
    print(g0.serialize(format="nt") + "\n")
    print(g1.serialize(format="nt") + "\n")
    g0.close()
    shutil.rmtree("/tmp/foo")
    g1.close()
    os.unlink("%(here)s/development.sqlite" % {"here": os.getcwd()})

if __name__ == "__main__":
    investigate_len_issue()
