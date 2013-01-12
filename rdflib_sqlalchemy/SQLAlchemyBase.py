"""
SQLAlchemyBase.py
SQLAlchemy declarative Base implementation of RDFLib Store.
"""

__metaclass__ = type

import logging
import re
#import uuid
#from urllib import quote, unquote
from rdflib.store import Store
#from rdflib.term import Literal, URIRef, BNode
#from rdflib.namespace import Namespace
#import sqlalchemy
from sqlalchemy import *
from sqlalchemy import sql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.ERROR, format="%(message)s")
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy').setLevel(logging.WARN)


_literal = re.compile(
    r'''"(?P<value>[^@&]*)"(?:@(?P<lang>[^&]*))?(?:&<(?P<datatype>.*)>)?''')

global metadata
global Session
metadata = MetaData()
Base = declarative_base()
Session = sessionmaker()

LITERAL = 0
URI = 1
NO_URI = 'uri://dev/null/'
Any = None

TABLEPREFIX = 'sqla_'


class Namespaces(Base):
    __tablename__ = TABLEPREFIX + 'namespaces'
    prefix = Column(String(1000), index=True, primary_key=True)
    ns = Column(String(1000), index=True)

    def __repr__(self):
        return "<Namespace(%s: %s)>" % (self.prefix, self.ns)
_namespaces = Namespaces


class Triples(Base):
    __tablename__ = TABLEPREFIX + 'triples'
    subject = Column(UnicodeText, index=True, primary_key=True)
    predicate = Column(UnicodeText, index=True, primary_key=True)
    object = Column(UnicodeText, index=True, primary_key=True)
    context = Column(UnicodeText, index=True, primary_key=True)
    quoted = Column(Boolean, default=False, index=True, primary_key=True)

    def __repr__(self):
        return "<Triple(%s, %s, %s, %s)>" % (
            self.subject, self.predicate, self.object, self.context)
_triples = Triples

Index('idx_ns_prefix',
    Namespaces.ns, Namespaces.prefix)
Index('idx_subject_predicate',
    Triples.subject, Triples.predicate)
Index('idx_predicate_object',
    Triples.predicate, Triples.object)
Index('idx_subject_object_context',
    Triples.subject, Triples.object, Triples.context)
Index('idx_predicate_object_context',
    Triples.predicate, Triples.object, Triples.context)
Index('idx_subject_predicate_context',
    Triples.subject, Triples.predicate, Triples.context)
Index('idx_subject_predicate_object_context',
    Triples.subject, Triples.predicate, Triples.object, Triples.context)


class SQLAlchemy(Store):

    context_aware = True
    formula_aware = True
    __open = False
    _triples = Triples
    _ns = Namespaces
    __node_pickler = None
    _connection = None
    echo = False

    tables = ('_triples', '_namespaces')

    def __init__(self, identifier=None, configuration=None):
        self.uri = configuration
        self.identifier = identifier
        super(SQLAlchemy, self).__init__(
            identifier=identifier, configuration=configuration)

    def open(self, configuration='sqlite://:memory:', create=True):
        if self.__open:
            return
        self.__open = True

        from sqlalchemy import create_engine
        engine = create_engine(configuration, echo=self.echo)
        Base.metadata.bind = engine
        if create:
            try:
                Base.metadata.create_all(checkfirst=True)
            except Exception as e:  # TODO: catch more specific exception
                print(e)
                _logger.warning(e)
                return 0
        Session.configure(bind=engine)
        self.dbsession = Session()
        self._connection = engine.connect()
        # useful for debugging
        self._connection.debug = False
        self.conn = self._connection
        _logger.debug("Graph opened, %s engine bound." % (engine.name))
        return True

    def close(self, commit_pending_transaction=False):
        if not self.__open:
            raise ValueError('Not open')
        self.__open = False
        if commit_pending_transaction:
            self.commit()
        self.dbsession.close()

    def destroy(self, configuration='sqlite://:memory:'):
        if self.__open:
            return
        from sqlalchemy import create_engine
        Base.metadata.bind = create_engine(configuration)
        try:
            Base.metadata.drop_all(checkfirst=False)
        except Exception as e:  # TODO: catch more specific exception
            print(e)
            _logger.warning(e)
            return 0
        return 1

    #RDF APIs
    def add(self, (subject, predicate, object), context, quoted=False):
        """
        Adds the given statement to a specific context or to the model. The
        quoted argument is interpreted by formula-aware stores to indicate
        this statement is quoted/hypothetical It should be an error to not
        specify a context and have the quoted argument be True. It should also
        be an error for the quoted argument to be True when the store is not
        formula-aware.
        """
        Store.add(self, (subject, predicate, object), context, quoted)
        self.dbsession.add(Triples(
            subject=subject.n3(), predicate=predicate.n3(), object=object.n3(),
            context=context.n3(), quoted=quoted))
        self.commit()

    def remove(self, (subject, predicate, object), context=None):
        """ Remove the set of triples matching the pattern from the store """
        Store.remove(self, (subject, predicate, object), context)
        triple = self.dbsession.query(Triples).filter(
                        subject=subject.n3(),
                        predicate=predicate.n3(),
                        object=object.n3(),
                        context=context.n3()).one()
        self.dbsession.delete(triple)
        self.commit()

    def triples_choices(self, triple, context=None):
        """
        A variant of triples that can take a list of terms instead of a single
        term in any slot.  Stores can implement this to optimize the response
        time from the default 'fallback' implementation, which will iterate
        over each term in the list and dispatch to triples().
        """
        (subject, predicate, object_) = triple
        if isinstance(object_, list):
            assert not isinstance(subject, list), \
                "object_ / subject are both lists"
            assert not isinstance(predicate, list), \
                "object_ / predicate are both lists"
            if object_:
                for obj in object_:
                    for (s1, p1, o1), cg in self.triples(
                            (subject, predicate, obj), context):
                        yield (s1, p1, o1), cg
            else:
                for (s1, p1, o1), cg in self.triples(
                        (subject, predicate, None), context):
                    yield (s1, p1, o1), cg

        elif isinstance(subject, list):
            assert not isinstance(predicate, list), \
                "subject / predicate are both lists"
            if subject:
                for subj in subject:
                    for (s1, p1, o1), cg in self.triples(
                            (subj, predicate, object_), context):
                        yield (s1, p1, o1), cg
            else:
                for (s1, p1, o1), cg in self.triples(
                        (None, predicate, object_), context):
                    yield (s1, p1, o1), cg

        elif isinstance(predicate, list):
            assert not isinstance(subject, list), \
                "predicate / subject are both lists"
            if predicate:
                for pred in predicate:
                    for (s1, p1, o1), cg in self.triples(
                            (subject, pred, object_), context):
                        yield (s1, p1, o1), cg
            else:
                for (s1, p1, o1), cg in self.triples(
                        (subject, None, object_), context):
                    yield (s1, p1, o1), cg

    def triples(self, triple_pattern, context=None):
        """
        A generator over all the triples matching the pattern. Pattern can
        include any objects for used for comparing against nodes in the store,
        for example, REGEXTerm, URIRef, Literal, BNode, Variable, Graph,
        QuotedGraph, Date? DateRange?

        A conjunctive query can be indicated by either providing a value of
        None for the context or the identifier associated with the Conjunctive
        Graph (if it's context aware).
        """
        subject, predicate, object = triple_pattern
        return self.dbsession.query(
            Triples).filter_by(
                subject=subject, predicate=predicate, object=object,
                context=context)

    # variants of triples will be done if / when optimization is needed

    def __len__(self, context=None):
        """
        Number of statements in the store. This should only account for non-
        quoted (asserted) statements if the context is not specified,
        otherwise it should return the number of statements in the formula or
        context given.
        """
        if context is not None:
            return self.dbsession.query(
                Triples).filter(quoted!=True).all()
        else:
            return self.dbsession.query(
                Triples).filter(context=context).all()

    def contexts(self, triple=None):
        """
        Generator over all contexts in the graph. If triple is specified, a
        generator over all contexts the triple is in.
        """
        if triple is not None:
            pass
        else:
            return self.dbsession.query(
                Triples).select(context).distinct().all()

    # Optional Namespace methods

    def bind(self, prefix, namespace):
        """ """
        self.dbsession.add(Namespaces(prefix=prefix, namespace=namespace))
        self.commit()

    def prefix(self, namespace):
        """ """
        return self.session.query(
            Namespaces).filter(namespace=namespace).one().prefix

    def namespace(self, prefix):
        """ """
        return self.dbsession.query(
            Namespaces).filter(prefix=prefix).one().namespace

    def namespaces(self):
        """ """
        for ns in self.dbsession.query(Namespaces).all():
            yield ns

    # Optional Transactional methods

    def commit(self):
        """ """
        self.dbsession.commit()

    def rollback(self):
        """ """
        self.dbsession.rollback()
