"""SQLAlchemy-based RDF store."""
from __future__ import with_statement

import hashlib
import logging
import re
import sys

import sqlalchemy
from rdflib import (
    BNode,
    Literal,
    RDF,
    URIRef
)
from rdflib.graph import Graph
from rdflib.graph import QuotedGraph
from rdflib.plugins.stores.regexmatching import PYTHON_REGEX
from rdflib.plugins.stores.regexmatching import REGEXTerm
from rdflib.store import Store
from rdflib.term import Node
from six import text_type
from six.moves import reduce
from six.moves.urllib.parse import unquote_plus
from sqlalchemy import Column, Table, MetaData, Index, types
from sqlalchemy.sql import select, expression

from .termutils import REVERSE_TERM_COMBINATIONS
from .termutils import TERM_INSTANTIATION_DICT
from .termutils import constructGraph
from .termutils import type2TermCombination
from .termutils import statement2TermCombination
from . import __version__


_logger = logging.getLogger(__name__)

COUNT_SELECT = 0
CONTEXT_SELECT = 1
TRIPLE_SELECT = 2
TRIPLE_SELECT_NO_ORDER = 3

ASSERTED_NON_TYPE_PARTITION = 3
ASSERTED_TYPE_PARTITION = 4
QUOTED_PARTITION = 5
ASSERTED_LITERAL_PARTITION = 6

FULL_TRIPLE_PARTITIONS = [QUOTED_PARTITION, ASSERTED_LITERAL_PARTITION]

INTERNED_PREFIX = "kb_"

MYSQL_MAX_INDEX_LENGTH = 200

Any = None

# Stolen from Will Waites' py4s


def skolemise(statement):
    """Skolemise."""
    def _sk(x):
        if isinstance(x, BNode):
            return URIRef("bnode:%s" % x)
        return x
    return tuple(map(_sk, statement))


def deskolemise(statement):
    """Deskolemise."""
    def _dst(x):
        if isinstance(x, URIRef) and x.startswith("bnode:"):
            _unused, bnid = x.split(":", 1)
            return BNode(bnid)
        return x
    return tuple(map(_dst, statement))


def regexp(expr, item):
    """User-defined REGEXP operator."""
    r = re.compile(expr)
    return r.match(item) is not None


def _parse_rfc1738_args(name):
    import cgi
    """ parse url str into options
    code orig from sqlalchemy.engine.url """
    pattern = re.compile(r"""
            (?P<name>[\w\+]+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>[^/]*))?
            @)?
            (?:
                (?P<host>[^/:]*)
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            """, re.X)

    m = pattern.match(name)
    if m is not None:
        (name, username, password, host, port, database) = m.group(
            1, 2, 3, 4, 5, 6)
        if database is not None:
            tokens = database.split(r"?", 2)
            database = tokens[0]
            query = (
                len(tokens) > 1 and dict(cgi.parse_qsl(tokens[1])) or None)
            if query is not None:
                query = dict([(k.encode("ascii"), query[k]) for k in query])
        else:
            query = None
        opts = {"username": username, "password": password, "host":
                host, "port": port, "database": database, "query": query}
        if opts["password"] is not None:
            opts["password"] = unquote_plus(opts["password"])
        return (name, opts)
    else:
        raise ValueError("Could not parse rfc1738 URL from string '%s'" % name)


def queryAnalysis(query, store, connection):
    """
    Helper function.

    For executing EXPLAIN on all dispatched SQL statements -
    for the pupose of analyzing index usage
    """
    res = connection.execute("explain " + query)
    rt = res.fetchall()[0]
    table, joinType, posKeys, _key, key_len, \
        comparedCol, rowsExamined, extra = rt
    if not _key:
        assert joinType == "ALL"
        if not hasattr(store, "queryOptMarks"):
            store.queryOptMarks = {}
        hits = store.queryOptMarks.get(("FULL SCAN", table), 0)
        store.queryOptMarks[("FULL SCAN", table)] = hits + 1

    if not hasattr(store, "queryOptMarks"):
        store.queryOptMarks = {}
    hits = store.queryOptMarks.get((_key, table), 0)
    store.queryOptMarks[(_key, table)] = hits + 1


def unionSELECT(selectComponents, distinct=False, selectType=TRIPLE_SELECT):
    """
    Helper function for building union all select statement.

    Terms: u - uri refs  v - variables  b - bnodes l - literal f - formula

    Takes a list of:
     - table name
     - table alias
     - table type (literal, type, asserted, quoted)
     - where clause string
    """
    selects = []
    for table, whereClause, tableType in selectComponents:

        if selectType == COUNT_SELECT:
            selectClause = table.count(whereClause)
        elif selectType == CONTEXT_SELECT:
            selectClause = expression.select([table.c.context], whereClause)
        elif tableType in FULL_TRIPLE_PARTITIONS:
            selectClause = table.select(whereClause)
        elif tableType == ASSERTED_TYPE_PARTITION:
            selectClause = expression.select(
                [table.c.id.label("id"),
                 table.c.member.label("subject"),
                 expression.literal(text_type(RDF.type)).label("predicate"),
                 table.c.klass.label("object"),
                 table.c.context.label("context"),
                 table.c.termComb.label("termcomb"),
                 expression.literal_column("NULL").label("objlanguage"),
                 expression.literal_column("NULL").label("objdatatype")][1 if __version__ <= "0.2" else 0:],
                whereClause)
        elif tableType == ASSERTED_NON_TYPE_PARTITION:
            selectClause = expression.select(
                [c for c in table.columns] +
                [expression.literal_column("NULL").label("objlanguage"),
                 expression.literal_column("NULL").label("objdatatype")],
                whereClause,
                from_obj=[table])

        selects.append(selectClause)

    orderStmt = []
    if selectType == TRIPLE_SELECT:
        orderStmt = [expression.literal_column("subject"),
                     expression.literal_column("predicate"),
                     expression.literal_column("object")]
    if distinct:
        return expression.union(*selects, **{"order_by": orderStmt})
    else:
        return expression.union_all(*selects, **{"order_by": orderStmt})


def extractTriple(tupleRt, store, hardCodedContext=None):
    """
    Extract a triple.

    Take a tuple which represents an entry in a result set and
    converts it to a tuple of terms using the termComb integer
    to interpret how to instantiate each term
    """
    if __version__ <= "0.2":
        try:
            subject, predicate, obj, rtContext, termComb, \
                objLanguage, objDatatype = tupleRt
            termCombString = REVERSE_TERM_COMBINATIONS[termComb]
            subjTerm, predTerm, objTerm, ctxTerm = termCombString
        except ValueError:
            subject, subjTerm, predicate, predTerm, obj, objTerm, \
                rtContext, ctxTerm, objLanguage, objDatatype = tupleRt
    else:
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
    s = createTerm(subject, subjTerm, store)
    p = createTerm(predicate, predTerm, store)
    o = createTerm(obj, objTerm, store, objLanguage, objDatatype)

    graphKlass, idKlass = constructGraph(ctxTerm)
    if __version__ <= "0.2":
        return s, p, o, (graphKlass, idKlass, context)
    else:
        return id, s, p, o, (graphKlass, idKlass, context)


def createTerm(
        termString, termType, store, objLanguage=None, objDatatype=None):
    # TODO: Stuff
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


class TermType(types.TypeDecorator):
    """Term typology."""

    impl = types.Text

    def process_bind_param(self, value, dialect):
        """Process bound parameters."""
        if isinstance(value, (QuotedGraph, Graph)):
            return text_type(value.identifier)
        elif isinstance(value, Node):
            return text_type(value)
        else:
            return value


class SQLGenerator(object):
    """SQL statement generator."""

    def buildTypeSQLCommand(self, member, klass, context):
        """Build an insert command for a type table."""
        # columns: member,klass,context
        rt = self.tables["type_statements"].insert()
        return rt, {
            "member": member,
            "klass": klass,
            "context": context.identifier,
            "termComb": int(type2TermCombination(member, klass, context))}

    def buildLiteralTripleSQLCommand(
            self, subject, predicate, obj, context):
        """
        Build an insert command for literal triples.

        (Statements where the object is a Literal).
        """
        triplePattern = int(
            statement2TermCombination(subject, predicate, obj, context))
        command = self.tables["literal_statements"].insert()
        values = {
            "subject": subject,
            "predicate": predicate,
            "object": obj,
            "context": context.identifier,
            "termComb": triplePattern,
            "objLanguage": isinstance(obj, Literal) and obj.language or None,
            "objDatatype": isinstance(obj, Literal) and obj.datatype or None
        }
        return command, values

    def buildTripleSQLCommand(
            self, subject, predicate, obj, context, quoted):
        """Build an insert command for regular triple table."""
        stmt_table = quoted and self.tables["quoted_statements"] \
            or self.tables["asserted_statements"]
        triplePattern = statement2TermCombination(
            subject, predicate, obj, context)
        command = stmt_table.insert()
        if quoted:
            params = {
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "context": context.identifier,
                "termComb": triplePattern,
                "objLanguage": isinstance(
                    obj, Literal) and obj.language or None,
                "objDatatype": isinstance(
                    obj, Literal) and obj.datatype or None
            }
        else:
            params = {
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "context": context.identifier,
                "termComb": triplePattern
            }
        return command, params

    def buildClause(
            self, table, subject, predicate, obj, context=None,
            typeTable=False):
        """Build WHERE clauses for the supplied terms and, context."""
        if typeTable:
            clauseList = [
                self.buildTypeMemberClause(subject, table),
                self.buildTypeClassClause(obj, table),
                self.buildContextClause(context, table)
            ]
        else:
            clauseList = [
                self.buildSubjClause(subject, table),
                self.buildPredClause(predicate, table),
                self.buildObjClause(obj, table),
                self.buildContextClause(context, table),
                self.buildLitDTypeClause(obj, table),
                self.buildLitLanguageClause(obj, table)
            ]

        clauseList = [clause for clause in clauseList if clause is not None]
        if clauseList:
            return expression.and_(*clauseList)
        else:
            return None

    def buildLitDTypeClause(self, obj, table):
        """Build Literal and datatype clause."""
        if isinstance(obj, Literal) and obj.datatype is not None:
            return table.c.objDatatype == obj.datatype
        else:
            return None

    def buildLitLanguageClause(self, obj, table):
        """Build Literal and language clause."""
        if isinstance(obj, Literal) and obj.language is not None:
            return table.c.objLanguage == obj.language
        else:
            return None

    # Where Clause  utility Functions
    # The predicate and object clause builders are modified in order
    # to optimize subjects and objects utility functions which can
    # take lists as their last argument (object, predicate -
    # respectively)
    def buildSubjClause(self, subject, table):
        """Build Subject clause."""
        if isinstance(subject, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.subject.op("REGEXP")(subject)
        elif isinstance(subject, list):
            # clauseStrings = [] --- unused
            return expression.or_(
                *[self.buildSubjClause(s, table) for s in subject if s])
        elif isinstance(subject, (QuotedGraph, Graph)):
            return table.c.subject == subject.identifier
        elif subject is not None:
            return table.c.subject == subject
        else:
            return None

    def buildPredClause(self, predicate, table):
        """
        Build Predicate clause.

        Capable of taking a list of predicates as well (in which case
        subclauses are joined with 'OR')
        """
        if isinstance(predicate, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.predicate.op("REGEXP")(predicate)
        elif isinstance(predicate, list):
            return expression.or_(
                *[self.buildPredClause(p, table) for p in predicate if p])
        elif predicate is not None:
            return table.c.predicate == predicate
        else:
            return None

    def buildObjClause(self, obj, table):
        """
        Build Object clause.

        Capable of taking a list of objects as well (in which case subclauses
        are joined with 'OR')
        """
        if isinstance(obj, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.object.op("REGEXP")(obj)
        elif isinstance(obj, list):
            return expression.or_(
                *[self.buildObjClause(o, table) for o in obj if o])
        elif isinstance(obj, (QuotedGraph, Graph)):
            return table.c.object == obj.identifier
        elif obj is not None:
            return table.c.object == obj
        else:
            return None

    def buildContextClause(self, context, table):
        """Build Context clause."""
        if isinstance(context, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.context.op("regexp")(context.identifier)
        elif context is not None and context.identifier is not None:
            return table.c.context == context.identifier
        else:
            return None

    def buildTypeMemberClause(self, subject, table):
        """Build Type Member clause."""
        if isinstance(subject, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.member.op("regexp")(subject)
        elif isinstance(subject, list):
            return expression.or_(
                *[self.buildTypeMemberClause(s, table) for s in subject if s])
        elif subject is not None:
            return table.c.member == subject
        else:
            return None

    def buildTypeClassClause(self, obj, table):
        """Build Type Class clause."""
        if isinstance(obj, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.klass.op("regexp")(obj)
        elif isinstance(obj, list):
            return expression.or_(
                *[self.buildTypeClassClause(o, table) for o in obj if o])
        elif obj is not None:
            return obj and table.c.klass == obj
        else:
            return None


class SQLAlchemy(Store, SQLGenerator):
    """
    SQL-92 formula-aware implementation of an rdflib Store.

    It stores its triples in the following partitions:

    - Asserted non rdf:type statements
    - Asserted literal statements
    - Asserted rdf:type statements (in a table which models Class membership)
        The motivation for this partition is primarily query speed and
        scalability as most graphs will always have more rdf:type statements
        than others
    - All Quoted statements

    In addition it persists namespace mappings in a separate table
    """

    context_aware = True
    formula_aware = True
    transaction_aware = True
    regex_matching = PYTHON_REGEX
    configuration = Literal("sqlite://")

    def __init__(self, identifier=None, configuration=None):
        """
        Initialisation.

        identifier: URIRef of the Store. Defaults to CWD
        configuration: string containing infomation open can use to
        connect to datastore.
        """
        self.identifier = identifier and identifier or "hardcoded"
        # Use only the first 10 bytes of the digest
        self._internedId = INTERNED_PREFIX + \
            hashlib.sha1(
                self.identifier.encode("utf8")).hexdigest()[:10]

        # This parameter controls how exlusively the literal table is searched
        # If true, the Literal partition is searched *exclusively* if the
        # object term in a triple pattern is a Literal or a REGEXTerm.  Note,
        # the latter case prevents the matching of URIRef nodes as the objects
        # of a triple in the store.
        # If the object term is a wildcard (None)
        # Then the Literal paritition is searched in addition to the others
        # If this parameter is false, the literal partition is searched
        # regardless of what the object of the triple pattern is
        self.STRONGLY_TYPED_TERMS = False

        self.cacheHits = 0
        self.cacheMisses = 0
        self.literalCache = {}
        self.uriCache = {}
        self.bnodeCache = {}
        self.otherCache = {}
        self.__node_pickler = None

        self.__create_table_definitions()

        if configuration is not None:
            self.configuration = configuration
            self.open(configuration)

    def __get_node_pickler(self):
        if getattr(self, "__node_pickler", False) \
                or self.__node_pickler is None:
            from rdflib.term import URIRef
            from rdflib.graph import GraphValue
            from rdflib.term import Variable
            from rdflib.term import Statement
            from rdflib.store import NodePickler
            self.__node_pickler = np = NodePickler()
            np.register(self, "S")
            np.register(URIRef, "U")
            np.register(BNode, "B")
            np.register(Literal, "L")
            np.register(Graph, "G")
            np.register(QuotedGraph, "Q")
            np.register(Variable, "V")
            np.register(Statement, "s")
            np.register(GraphValue, "v")
        return self.__node_pickler
    node_pickler = property(__get_node_pickler)

    def open(self, configuration, create=True):
        """
        Open the store specified by the configuration string.

        If create is True a store will be created if it does not already
        exist. If create is False and a store does not already exist
        an exception is raised. An exception is also raised if a store
        exists, but there is insufficient permissions to open the
        store.
        """
        name, opts = _parse_rfc1738_args(configuration)

        self.engine = sqlalchemy.create_engine(configuration)
        with self.engine.connect() as connection:
            assert connection is not None
            if create:
                self.metadata.create_all(self.engine)
        # self._db.create_function("regexp", 2, regexp)
        if configuration:
            from sqlalchemy.engine import reflection
            insp = reflection.Inspector.from_engine(self.engine)
            tbls = insp.get_table_names()
            for tn in [tbl % (self._internedId)
                       for tbl in table_name_prefixes]:
                if tn not in tbls:
                    sys.stderr.write("table %s Doesn't exist\n" % (tn))
                    # The database exists, but one of the partitions
                    # doesn't exist
                    return 0
            # Everything is there (the database and the partitions)
            return 1
        # The database doesn't exist - nothing is there
        return -1

    def close(self, commit_pending_transaction=False):
        """FIXME:  Add documentation."""
        try:
            self.engine.close()
        except:
            pass

    def destroy(self, configuration):
        """FIXME: Add documentation."""
        name, opts = _parse_rfc1738_args(configuration)
        if self.engine is None:
            # _logger.debug("Connecting in order to destroy.")
            self.engine = sqlalchemy.create_engine(configuration)
        #     _logger.debug("Connected")
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                self.metadata.drop_all(self.engine)
                trans.commit()
            except Exception:
                e = sys.exc_info()[1]
                msg = e.args[0] if len(e.args) > 0 else ""
                _logger.debug("unable to drop table: %s " % (msg))
                trans.rollback()
        # Note, this only removes the associated tables for the closed
        # world universe given by the identifier
        # _logger.debug(
        #       "Destroyed Close World Universe %s" % (self.identifier))

    def __getBuildCommand(self, triple, context=None, quoted=False):

        subject, predicate, obj = triple
        buildCommandType = None
        if quoted or predicate != RDF.type:
            # Quoted statement or non rdf:type predicate
            # check if object is a literal
            if isinstance(obj, Literal):
                addCmd, params = self.buildLiteralTripleSQLCommand(
                    subject, predicate, obj, context)
                buildCommandType = "literal"
            else:
                addCmd, params = self.buildTripleSQLCommand(
                    subject, predicate, obj, context, quoted)
                buildCommandType = "other"
        elif predicate == RDF.type:
            # asserted rdf:type statement
            addCmd, params = self.buildTypeSQLCommand(subject, obj, context)
            buildCommandType = "type"
        return buildCommandType, addCmd, params

    # Triple Methods
    def add(self, triple, context=None, quoted=False):
        """Add a triple to the store of triples."""
        subject, predicate, obj = triple
        _, addCmd, params = self.__getBuildCommand(
            (subject, predicate, obj), context, quoted)
        with self.engine.connect() as connection:
            try:
                connection.execute(addCmd, params)
            except Exception:
                e = sys.exc_info()[1]
                msg = e.args[0] if len(e.args) > 0 else ""
                _logger.debug(
                    "Add failed %s with commands %s params %s" % (
                        msg, str(addCmd), repr(params)))
                raise

    def addN(self, quads):
        """Add a list of triples in quads form."""
        cmdTripleDict = {}

        for subject, predicate, obj, context in quads:
            buildCommandType, cmd, params = \
                self.__getBuildCommand(
                    (subject, predicate, obj),
                    context,
                    isinstance(context, QuotedGraph))

            cmdTriple = cmdTripleDict.setdefault(buildCommandType, {})
            cmdTriple.setdefault("cmd", cmd)
            cmdTriple.setdefault("params", []).append(params)

        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                for cmdTriple in cmdTripleDict.values():
                    connection.execute(cmdTriple["cmd"], cmdTriple["params"])
                trans.commit()
            except Exception:
                e = sys.exc_info()[1]
                msg = e.args[0] if len(e.args) > 0 else ""
                _logger.debug("AddN failed %s" % msg)
                trans.rollback()
                raise

    def remove(self, triple, context):
        """Remove a triple from the store."""
        subject, predicate, obj = triple
        if context is not None:
            if subject is None and predicate is None and object is None:
                self._remove_context(context)
                return
        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                if not predicate or predicate != RDF.type:
                    # Need to remove predicates other than rdf:type

                    if not self.STRONGLY_TYPED_TERMS \
                            or isinstance(obj, Literal):
                        # remove literal triple
                        clause = self.buildClause(
                            literal_table, subject, predicate, obj, context)
                        connection.execute(literal_table.delete(clause))

                    for table in [quoted_table, asserted_table]:
                        # If asserted non rdf:type table and obj is Literal,
                        # don't do anything (already taken care of)
                        if table == asserted_table \
                                and isinstance(obj, Literal):
                            continue
                        else:
                            clause = self.buildClause(
                                table, subject, predicate, obj, context)
                            connection.execute(table.delete(clause))

                if predicate == RDF.type or not predicate:
                    # Need to check rdf:type and quoted partitions (in addition
                    # perhaps)
                    clause = self.buildClause(
                        asserted_type_table, subject,
                        RDF.type, obj, context, True)
                    connection.execute(asserted_type_table.delete(clause))

                    clause = self.buildClause(
                        quoted_table, subject, predicate, obj, context)
                    connection.execute(quoted_table.delete(clause))

                trans.commit()
            except Exception:
                e = sys.exc_info()[1]
                msg = e.args[0] if len(e.args) > 0 else ""
                _logger.debug("Removal failed %s" % msg)
                trans.rollback()

    def triples(self, triple, context=None):
        """
        A generator over all the triples matching pattern.

        Pattern can be any objects for comparing against nodes in
        the store, for example, RegExLiteral, Date? DateRange?

        quoted table:                <id>_quoted_statements
        asserted rdf:type table:     <id>_type_statements
        asserted non rdf:type table: <id>_asserted_statements

        triple columns: subject,predicate,object,context,termComb,
                        objLanguage,objDatatype
        class membership columns: member,klass,context termComb

        FIXME:  These union all selects *may* be further optimized by joins

        """
        subject, predicate, obj = triple

        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]

        if predicate == RDF.type:
            # select from asserted rdf:type partition and quoted table
            # (if a context is specified)
            typeTable = expression.alias(
                asserted_type_table, "typetable")
            clause = self.buildClause(
                typeTable, subject, RDF.type, obj, context, True)
            selects = [
                (typeTable,
                 clause,
                 ASSERTED_TYPE_PARTITION), ]

        elif isinstance(predicate, REGEXTerm) \
                and predicate.compiledExpr.match(RDF.type) \
                or not predicate:
            # Select from quoted partition (if context is specified),
            # Literal partition if (obj is Literal or None) and asserted
            # non rdf:type partition (if obj is URIRef or None)
            selects = []
            if not self.STRONGLY_TYPED_TERMS \
                    or isinstance(obj, Literal) \
                    or not obj \
                    or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm)):
                literal = expression.alias(literal_table, "literal")
                clause = self.buildClause(
                    literal, subject, predicate, obj, context)
                selects.append((literal, clause, ASSERTED_LITERAL_PARTITION))

            if not isinstance(obj, Literal) \
                    and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                asserted = expression.alias(asserted_table, "asserted")
                clause = self.buildClause(
                    asserted, subject, predicate, obj, context)
                selects.append((asserted, clause, ASSERTED_NON_TYPE_PARTITION))

            typeTable = expression.alias(asserted_type_table, "typetable")
            clause = self.buildClause(
                typeTable, subject, RDF.type, obj, context, True)
            selects.append((typeTable, clause, ASSERTED_TYPE_PARTITION))

        elif predicate:
            # select from asserted non rdf:type partition (optionally),
            # quoted partition (if context is specified), and literal
            # partition (optionally)
            selects = []
            if not self.STRONGLY_TYPED_TERMS \
                    or isinstance(obj, Literal) \
                    or not obj \
                    or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm)):
                literal = expression.alias(literal_table, "literal")
                clause = self.buildClause(
                    literal, subject, predicate, obj, context)
                selects.append((literal, clause, ASSERTED_LITERAL_PARTITION))

            if not isinstance(obj, Literal) \
                    and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                asserted = expression.alias(asserted_table, "asserted")
                clause = self.buildClause(
                    asserted, subject, predicate, obj, context)
                selects.append((asserted, clause, ASSERTED_NON_TYPE_PARTITION))

        if context is not None:
            quoted = expression.alias(quoted_table, "quoted")
            clause = self.buildClause(quoted, subject, predicate, obj, context)
            selects.append((quoted, clause, QUOTED_PARTITION))

        q = unionSELECT(selects, selectType=TRIPLE_SELECT_NO_ORDER)
        with self.engine.connect() as connection:
            # _logger.debug("Triples query : %s" % str(q))
            res = connection.execute(q)
            # TODO: False but it may have limitations on text column. Check
            # NOTE: SQLite does not support ORDER BY terms that aren't
            # integers, so the entire result set must be iterated in order
            # to be able to return a generator of contexts
            result = res.fetchall()
        tripleCoverage = {}
        for rt in result:
            if __version__ <= "0.2":
                s, p, o, (graphKlass, idKlass, graphId) = \
                    extractTriple(rt, self, context)
            else:
                id, s, p, o, (graphKlass, idKlass, graphId) = \
                    extractTriple(rt, self, context)
            contexts = tripleCoverage.get((s, p, o), [])
            contexts.append(graphKlass(self, idKlass(graphId)))
            tripleCoverage[(s, p, o)] = contexts

        for (s, p, o), contexts in tripleCoverage.items():
            yield (s, p, o), (c for c in contexts)

    def triples_choices(self, triple, context=None):
        """
        A variant of triples.

        Can take a list of terms instead of a single term in any slot.
        Stores can implement this to optimize the response time from the
        import default 'fallback' implementation, which will iterate over
        each term in the list and dispatch to triples.
        """
        subject, predicate, object_ = triple

        if isinstance(object_, list):
            assert not isinstance(
                subject, list), "object_ / subject are both lists"
            assert not isinstance(
                predicate, list), "object_ / predicate are both lists"
            if not object_:
                object_ = None
            for (s1, p1, o1), cg in self.triples(
                    (subject, predicate, object_), context):
                yield (s1, p1, o1), cg

        elif isinstance(subject, list):
            assert not isinstance(
                predicate, list), "subject / predicate are both lists"
            if not subject:
                subject = None
            for (s1, p1, o1), cg in self.triples(
                    (subject, predicate, object_), context):
                yield (s1, p1, o1), cg

        elif isinstance(predicate, list):
            assert not isinstance(
                subject, list), "predicate / subject are both lists"
            if not predicate:
                predicate = None
            for (s1, p1, o1), cg in self.triples(
                    (subject, predicate, object_), context):
                yield (s1, p1, o1), cg

    def __repr__(self):
        """Readable serialisation."""
        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]

        selects = [
            (expression.alias(asserted_type_table, "typetable"),
                None, ASSERTED_TYPE_PARTITION),
            (expression.alias(quoted_table, "quoted"),
                None, QUOTED_PARTITION),
            (expression.alias(asserted_table, "asserted"),
                None, ASSERTED_NON_TYPE_PARTITION),
            (expression.alias(literal_table, "literal"),
                None, ASSERTED_LITERAL_PARTITION), ]
        q = unionSELECT(selects, distinct=False, selectType=COUNT_SELECT)
        if hasattr(self, "engine"):
            with self.engine.connect() as connection:
                res = connection.execute(q)
                rt = res.fetchall()
                typeLen, quotedLen, assertedLen, literalLen = [
                    rtTuple[0] for rtTuple in rt]
            try:
                return ("<Partitioned SQL N3 Store: %s " +
                        "contexts, %s classification assertions, " +
                        "%s quoted statements, %s property/value " +
                        "assertions, and %s other assertions>" % (
                            len([ctx for ctx in self.contexts()]),
                            typeLen, quotedLen, literalLen, assertedLen))
            except Exception:
                return "<Partitioned SQL N3 Store>"
        else:
            return "<Partitioned unopened SQL N3 Store>"

    def __len__(self, context=None):
        """Number of statements in the store."""
        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]

        typetable = expression.alias(asserted_type_table, "typetable")
        quoted = expression.alias(quoted_table, "quoted")
        asserted = expression.alias(asserted_table, "asserted")
        literal = expression.alias(literal_table, "literal")

        quotedContext = self.buildContextClause(context, quoted)
        assertedContext = self.buildContextClause(context, asserted)
        typeContext = self.buildContextClause(context, typetable)
        literalContext = self.buildContextClause(context, literal)

        if context is not None:
            selects = [
                (typetable, typeContext,
                 ASSERTED_TYPE_PARTITION),
                (quoted, quotedContext,
                 QUOTED_PARTITION),
                (asserted, assertedContext,
                 ASSERTED_NON_TYPE_PARTITION),
                (literal, literalContext,
                 ASSERTED_LITERAL_PARTITION), ]
            q = unionSELECT(selects, distinct=True, selectType=COUNT_SELECT)
        else:
            selects = [
                (typetable, typeContext,
                 ASSERTED_TYPE_PARTITION),
                (asserted, assertedContext,
                 ASSERTED_NON_TYPE_PARTITION),
                (literal, literalContext,
                 ASSERTED_LITERAL_PARTITION), ]
            q = unionSELECT(selects, distinct=False, selectType=COUNT_SELECT)

        # _logger.debug("Length query : %s" % str(q))

        with self.engine.connect() as connection:
            res = connection.execute(q)
            rt = res.fetchall()
            # _logger.debug(rt)
            # _logger.debug(len(rt))
            return reduce(lambda x, y: x + y, [rtTuple[0] for rtTuple in rt])

    def contexts(self, triple=None):
        """Contexts."""
        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]

        typetable = expression.alias(asserted_type_table, "typetable")
        quoted = expression.alias(quoted_table, "quoted")
        asserted = expression.alias(asserted_table, "asserted")
        literal = expression.alias(literal_table, "literal")

        if triple is not None:
            subject, predicate, obj = triple
            if predicate == RDF.type:
                # Select from asserted rdf:type partition and quoted table
                # (if a context is specified)
                clause = self.buildClause(
                    typetable, subject, RDF.type, obj, Any, True)
                selects = [(typetable, clause, ASSERTED_TYPE_PARTITION), ]

            elif isinstance(predicate, REGEXTerm) \
                    and predicate.compiledExpr.match(RDF.type) \
                    or not predicate:
                # Select from quoted partition (if context is specified),
                # literal partition if (obj is Literal or None) and
                # asserted non rdf:type partition (if obj is URIRef
                # or None)
                clause = self.buildClause(
                    typetable, subject, RDF.type, obj, Any, True)
                selects = [(typetable, clause, ASSERTED_TYPE_PARTITION), ]

                if (not self.STRONGLY_TYPED_TERMS or
                        isinstance(obj, Literal) or
                        not obj or
                        (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm))):
                    clause = self.buildClause(literal, subject, predicate, obj)
                    selects.append(
                        (literal, clause, ASSERTED_LITERAL_PARTITION))
                if not isinstance(obj, Literal) \
                        and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                        or not obj:
                    clause = self.buildClause(
                        asserted, subject, predicate, obj)
                    selects.append(
                        (asserted, clause, ASSERTED_NON_TYPE_PARTITION))

            elif predicate:
                # select from asserted non rdf:type partition (optionally),
                # quoted partition (if context is speciied), and literal
                # partition (optionally)
                selects = []
                if (not self.STRONGLY_TYPED_TERMS or
                        isinstance(obj, Literal) or
                        not obj
                        or (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm))):
                    clause = self.buildClause(
                        literal, subject, predicate, obj)
                    selects.append(
                        (literal, clause, ASSERTED_LITERAL_PARTITION))
                if not isinstance(obj, Literal) \
                        and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                        or not obj:
                    clause = self.buildClause(
                        asserted, subject, predicate, obj)
                    selects.append(
                        (asserted, clause, ASSERTED_NON_TYPE_PARTITION))

            clause = self.buildClause(quoted, subject, predicate, obj)
            selects.append((quoted, clause, QUOTED_PARTITION))
            q = unionSELECT(selects, distinct=True, selectType=CONTEXT_SELECT)
        else:
            selects = [
                (typetable, None, ASSERTED_TYPE_PARTITION),
                (quoted, None, QUOTED_PARTITION),
                (asserted, None, ASSERTED_NON_TYPE_PARTITION),
                (literal, None, ASSERTED_LITERAL_PARTITION), ]
            q = unionSELECT(selects, distinct=True, selectType=CONTEXT_SELECT)

        with self.engine.connect() as connection:
            res = connection.execute(q)
            rt = res.fetchall()
        for context in [rtTuple[0] for rtTuple in rt]:
            yield URIRef(context)

    def _remove_context(self, identifier):
        """Remove context."""
        assert identifier
        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]

        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                for table in [quoted_table, asserted_table,
                              asserted_type_table, literal_table]:
                    clause = self.buildContextClause(identifier, table)
                    connection.execute(table.delete(clause))
                trans.commit()
            except Exception:
                e = sys.exc_info()[1]
                msg = e.args[0] if len(e.args) > 0 else ""
                _logger.debug("Context removal failed %s" % msg)
                trans.rollback()

    # Optional Namespace methods
    # Placeholder optimized interfaces (those needed in order to port Versa)
    def subjects(self, predicate=None, obj=None):
        """A generator of subjects with the given predicate and object."""
        raise Exception("Not implemented")

    # Capable of taking a list of predicate terms instead of a single term
    def objects(self, subject=None, predicate=None):
        """A generator of objects with the given subject and predicate."""
        raise Exception("Not implemented")

    # Optimized interfaces (others)
    def predicate_objects(self, subject=None):
        """A generator of (predicate, object) tuples for the given subject."""
        raise Exception("Not implemented")

    def subject_objects(self, predicate=None):
        """A generator of (subject, object) tuples for the given predicate."""
        raise Exception("Not implemented")

    def subject_predicates(self, object=None):
        """A generator of (subject, predicate) tuples for the given object."""
        raise Exception("Not implemented")

    def value(self, subject,
              predicate=u"http://www.w3.org/1999/02/22-rdf-syntax-ns#value",
              object=None, default=None, any=False):
        """
        Get a value.

        For a subject/predicate, predicate/object, or
        subject/object pair -- exactly one of subject, predicate,
        object must be None. Useful if one knows that there may only
        be one value.

        It is one of those situations that occur a lot, hence this
        'macro' like utility

        :param subject:
        :param predicate:
        :param object:  -- exactly one must be None
        :param default: -- value to be returned if no values found
        :param any: -- if true, return any value in the case there is more
                       than one, else raise a UniquenessError
        """
        raise Exception("Not implemented")

    # Namespace persistence interface implementation
    def bind(self, prefix, namespace):
        """Bind prefix for namespace."""
        with self.engine.connect() as connection:
            try:
                ins = self.tables["namespace_binds"].insert().values(
                    prefix=prefix, uri=namespace)
                connection.execute(ins)
            except Exception:
                e = sys.exc_info()[1]
                msg = e.args[0] if len(e.args) > 0 else ""
                _logger.debug("Namespace binding failed %s" % msg)

    def prefix(self, namespace):
        """Prefix."""
        with self.engine.connect() as connection:
            nb_table = self.tables["namespace_binds"]
            namespace = text_type(namespace)
            s = select([nb_table.c.prefix]).where(nb_table.c.uri == namespace)
            res = connection.execute(s)
            rt = [rtTuple[0] for rtTuple in res.fetchall()]
            res.close()
            return rt and rt[0] or None

    def namespace(self, prefix):
        """Namespace."""
        res = None
        prefix_val = text_type(prefix)
        try:
            with self.engine.connect() as connection:
                nb_table = self.tables["namespace_binds"]
                s = select([nb_table.c.uri]).where(nb_table.c.prefix == prefix_val)
                res = connection.execute(s)
                rt = [rtTuple[0] for rtTuple in res.fetchall()]
                res.close()
                # return rt and rt[0] or None
                from rdflib import URIRef
                return rt and URIRef(rt[0]) or None
        except:
            return None

    def namespaces(self):
        """Namespaces."""
        with self.engine.connect() as connection:
            res = connection.execute(self.tables["namespace_binds"].select())
            for prefix, uri in res.fetchall():
                yield prefix, uri

    def __create_table_definitions(self):
        self.metadata = MetaData()
        self.tables = {
            "asserted_statements":
            Table(
                "%s_asserted_statements" % self._internedId, self.metadata,
                Column("id", types.Integer, nullable=False, primary_key=True),
                Column("subject", TermType, nullable=False),
                Column("predicate", TermType, nullable=False),
                Column("object", TermType, nullable=False),
                Column("context", TermType, nullable=False),
                Column("termcomb", types.Integer,
                       nullable=False, key="termComb"),
                Index("%s_A_termComb_index" % self._internedId,
                      "termComb"),
                Index("%s_A_s_index" % self._internedId, "subject", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_A_p_index" % self._internedId, "predicate", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_A_o_index" % self._internedId, "object", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_A_c_index" % self._internedId, "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)),
            "type_statements":
            Table("%s_type_statements" % self._internedId, self.metadata,
                  Column("id", types.Integer, nullable=False, primary_key=True),
                  Column("member", TermType, nullable=False),
                  Column("klass", TermType, nullable=False),
                  Column("context", TermType, nullable=False),
                  Column("termcomb", types.Integer, nullable=False,
                         key="termComb"),
                  Index("%s_T_termComb_index" % self._internedId,
                        "termComb"),
                  Index("%s_member_index" % self._internedId, "member", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                  Index("%s_klass_index" % self._internedId, "klass", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                  Index("%s_c_index" % self._internedId, "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)),
            "literal_statements":
            Table(
                "%s_literal_statements" % self._internedId, self.metadata,
                Column("id", types.Integer, nullable=False, primary_key=True),
                Column("subject", TermType, nullable=False),
                Column("predicate", TermType, nullable=False),
                Column("object", TermType),
                Column("context", TermType, nullable=False),
                Column("termcomb", types.Integer, nullable=False,
                       key="termComb"),
                Column("objlanguage", types.String(255),
                       key="objLanguage"),
                Column("objdatatype", types.String(255),
                       key="objDatatype"),
                Index("%s_L_termComb_index" % self._internedId,
                      "termComb"),
                Index("%s_L_s_index" % self._internedId, "subject", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_L_p_index" % self._internedId, "predicate", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_L_c_index" % self._internedId, "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)),
            "quoted_statements":
            Table(
                "%s_quoted_statements" % self._internedId, self.metadata,
                Column("id", types.Integer, nullable=False, primary_key=True),
                Column("subject", TermType, nullable=False),
                Column("predicate", TermType, nullable=False),
                Column("object", TermType),
                Column("context", TermType, nullable=False),
                Column("termcomb", types.Integer, nullable=False,
                       key="termComb"),
                Column("objlanguage", types.String(255),
                       key="objLanguage"),
                Column("objdatatype", types.String(255),
                       key="objDatatype"),
                Index("%s_Q_termComb_index" % self._internedId,
                      "termComb"),
                Index("%s_Q_s_index" % self._internedId, "subject", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_Q_p_index" % self._internedId, "predicate", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_Q_o_index" % self._internedId, "object", mysql_length=MYSQL_MAX_INDEX_LENGTH),
                Index("%s_Q_c_index" % self._internedId, "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)),
            "namespace_binds":
            Table(
                "%s_namespace_binds" % self._internedId, self.metadata,
                Column("prefix", types.String(20), unique=True,
                       nullable=False, primary_key=True),
                Column("uri", types.Text),
                Index("%s_uri_index" % self._internedId, "uri", mysql_length=MYSQL_MAX_INDEX_LENGTH))
        }
        if __version__ > "0.2":
            for table in ["type_statements", "literal_statements",
                          "quoted_statements", "asserted_statements"]:
                self.tables[table].append_column(
                    Column("id", types.Integer, nullable=False, primary_key=True))

table_name_prefixes = [
    "%s_asserted_statements",
    "%s_type_statements",
    "%s_quoted_statements",
    "%s_namespace_binds",
    "%s_literal_statements"
]
