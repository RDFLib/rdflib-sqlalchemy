"""Convenience functions for working with Terms and Graphs."""
from rdflib import BNode, Graph, Literal, URIRef, Variable
from rdflib.graph import QuotedGraph

from rdflib_sqlalchemy.constants import (
    TERM_COMBINATIONS,
    TERM_INSTANTIATION_DICT,
    REVERSE_TERM_COMBINATIONS,
)

__all__ = ["extract_triple"]


SUBJECT = 0
PREDICATE = 1
OBJECT = 2
CONTEXT = 3

GRAPH_TERM_DICT = {
    "F": (QuotedGraph, URIRef),
    "U": (Graph, URIRef),
    "B": (Graph, BNode)
}


def normalize_graph(graph):
    """
    Take an instance of a ``Graph`` and return the instance's identifier and  ``type``.

    Types are ``U`` for a :class:`~rdflib.graph.Graph`, ``F`` for
    a :class:`~rdflib.graph.QuotedGraph` and ``B`` for a
    :class:`~rdflib.graph.ConjunctiveGraph`

    >>> from rdflib import plugin
    >>> from rdflib.graph import Graph, ConjunctiveGraph, QuotedGraph
    >>> from rdflib.store import Store
    >>> from rdflib import URIRef, Namespace
    >>> from rdflib_sqlalchemy.termutils import normalize_graph
    >>> memstore = plugin.get('IOMemory', Store)()
    >>> g = Graph(memstore, URIRef("http://purl.org/net/bel-epa/gjh"))
    >>> normalize_graph(g)
    (rdflib.term.URIRef(u'http://purl.org/net/bel-epa/gjh'), 'U')
    >>> g = ConjunctiveGraph(memstore, Namespace("http://rdflib.net/ns"))
    >>> normalize_graph(g)  #doctest: +ELLIPSIS
    (rdflib.term.URIRef(u'http://rdflib.net/ns'), 'U')
    >>> g = QuotedGraph(memstore, Namespace("http://rdflib.net/ns"))
    >>> normalize_graph(g)
    (rdflib.term.URIRef(u'http://rdflib.net/ns'), 'F')

    """
    if isinstance(graph, QuotedGraph):
        return graph.identifier, "F"
    else:
        return graph.identifier, term_to_letter(graph.identifier)


def term_to_letter(term):
    """
    Relate a given term to one of several key types.

    * :class:`~rdflib.term.BNode`,
    * :class:`~rdflib.term.Literal`,
    * :class:`~rdflib.term.URIRef`,
    * :class:`~rdflib.term.Variable`
    * :class:`~rdflib.graph.Graph`
    * :class:`~rdflib.graph.QuotedGraph`

    >>> from rdflib import URIRef
    >>> from rdflib.term import BNode
    >>> from rdflib.graph import Graph, QuotedGraph
    >>> from rdflib_sqlalchemy.termutils import term_to_letter
    >>> term_to_letter(URIRef('http://purl.org/net/bel-epa.com/'))
    'U'
    >>> term_to_letter(BNode())
    'B'
    >>> term_to_letter(Literal(u''))  # noqa
    'L'
    >>> term_to_letter(Variable(u'x'))  # noqa
    'V'
    >>> term_to_letter(Graph())
    'B'
    >>> term_to_letter(QuotedGraph("IOMemory", None))
    'F'
    >>> term_to_letter(None)
    'L'
    """
    if isinstance(term, URIRef):
        return "U"
    elif isinstance(term, BNode):
        return "B"
    elif isinstance(term, Literal):
        return "L"
    elif isinstance(term, QuotedGraph):
        return "F"
    elif isinstance(term, Variable):
        return "V"
    elif isinstance(term, Graph):
        return term_to_letter(term.identifier)
    elif term is None:
        return "L"
    else:
        raise Exception(
            ("The given term (%s) is not an instance of any " +
             "of the known types (URIRef, BNode, Literal, QuotedGraph, " +
             "or Variable).  It is a %s")
            % (term, type(term)))


def construct_graph(key):
    """
    Return a tuple containing a ``Graph`` and an appropriate referent.

    Takes a key (one of 'F', 'U' or 'B')

    >>> from rdflib_sqlalchemy.termutils import construct_graph
    >>> construct_graph('F')
    (<class 'rdflib.graph.QuotedGraph'>, <class 'rdflib.term.URIRef'>)
    >>> construct_graph('U')
    (<class 'rdflib.graph.Graph'>, <class 'rdflib.term.URIRef'>)
    >>> construct_graph('B')
    (<class 'rdflib.graph.Graph'>, <class 'rdflib.term.BNode'>)

    """
    return GRAPH_TERM_DICT[key]


def triple_pattern_to_term_combinations(triple):
    """Map a triple pattern to term combinations (non-functioning)."""
    s, p, o = triple
    combinations = []
    if isinstance(o, Literal):
        for key, val in TERM_COMBINATIONS.items():
            if key[OBJECT] == 'O':
                combinations.append(val)
    return combinations


def type_to_term_combination(member, klass, context):
    """Map a type to a term combination."""
    term_combination = '{subject}U{object}{context}'.format(
        subject=term_to_letter(member),
        object=term_to_letter(klass),
        context=normalize_graph(context)[-1],
    )

    try:
        return TERM_COMBINATIONS[term_combination]
    except KeyError:
        if isinstance(member, Literal):
            raise ValueError(
                'A Literal cannot be a subject of a triple.\n\n'
                'Triple causing error:\n'
                '  {member} rdf:type {klass}\n'
                'Context: {context}'.format(
                    member=member,
                    klass=klass,
                    context=context,
                )
            )

        raise


def statement_to_term_combination(subject, predicate, obj, context):
    """Map a statement to a Term Combo."""
    return TERM_COMBINATIONS["%s%s%s%s" %
                             (term_to_letter(subject), term_to_letter(predicate),
                              term_to_letter(obj), normalize_graph(context)[-1])]


def escape_quotes(qstr):
    """
    Escape backslashes.

    #FIXME:  This *may* prove to be a performance bottleneck and should
             perhaps be implemented in C (as it was in 4Suite RDF)

    Ported from Ft.Lib.DbUtil
    """
    if qstr is None:
        return ""
    tmp = qstr.replace("\\", "\\\\")
    tmp = tmp.replace("'", "\\'")
    return tmp


def extract_triple(tupleRt, store, hardCodedContext=None):
    """
    Extract a triple.

    Take a tuple which represents an entry in a result set and
    converts it to a tuple of terms using the termComb integer
    to interpret how to instantiate each term.

    """
    try:
        id, subject, predicate, obj, rtContext, termComb, \
            objLanguage, objDatatype = tupleRt
        termCombString = REVERSE_TERM_COMBINATIONS[termComb]
        subjTerm, predTerm, objTerm, ctxTerm = termCombString
    except ValueError:
        id, subject, subjTerm, predicate, predTerm, obj, objTerm, \
            rtContext, ctxTerm, objLanguage, objDatatype = tupleRt

    context = rtContext is not None \
        and rtContext \
        or hardCodedContext.identifier
    s = create_term(subject, subjTerm, store)
    p = create_term(predicate, predTerm, store)
    o = create_term(obj, objTerm, store, objLanguage, objDatatype)

    graphKlass, idKlass = construct_graph(ctxTerm)

    return id, s, p, o, (graphKlass, idKlass, context)


def create_term(termString, termType, store, objLanguage=None, objDatatype=None):
    """
    Take a term value, term type, and store instance and creates a term object.

    QuotedGraphs are instantiated differently
    """
    if termType == "L":
        cache = store.literalCache.get((termString, objLanguage, objDatatype))
        if cache is not None:
            # store.cacheHits += 1
            return cache
        else:
            # store.cacheMisses += 1
            # rt = Literal(termString, objLanguage, objDatatype)
            # store.literalCache[((termString, objLanguage, objDatatype))] = rt
            if objLanguage and not objDatatype:
                rt = Literal(termString, objLanguage)
                store.literalCache[((termString, objLanguage))] = rt
            elif objDatatype and not objLanguage:
                rt = Literal(termString, datatype=objDatatype)
                store.literalCache[((termString, objDatatype))] = rt
            elif not objLanguage and not objDatatype:
                rt = Literal(termString)
                store.literalCache[((termString))] = rt
            else:
                rt = Literal(termString, objDatatype)
                store.literalCache[((termString, objDatatype))] = rt
            return rt
    elif termType == "F":
        cache = store.otherCache.get((termType, termString))
        if cache is not None:
            # store.cacheHits += 1
            return cache
        else:
            # store.cacheMisses += 1
            rt = QuotedGraph(store, URIRef(termString))
            store.otherCache[(termType, termString)] = rt
            return rt
    elif termType == "B":
        cache = store.bnodeCache.get((termString))
        if cache is not None:
            # store.cacheHits += 1
            return cache
        else:
            # store.cacheMisses += 1
            rt = TERM_INSTANTIATION_DICT[termType](termString)
            store.bnodeCache[(termString)] = rt
            return rt
    elif termType == "U":
        cache = store.uriCache.get((termString))
        if cache is not None:
            # store.cacheHits += 1
            return cache
        else:
            # store.cacheMisses += 1
            rt = URIRef(termString)
            store.uriCache[(termString)] = rt
            return rt
    else:
        cache = store.otherCache.get((termType, termString))
        if cache is not None:
            # store.cacheHits += 1
            return cache
        else:
            # store.cacheMisses += 1
            rt = TERM_INSTANTIATION_DICT[termType](termString)
            store.otherCache[(termType, termString)] = rt
            return rt
