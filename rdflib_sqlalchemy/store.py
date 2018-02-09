"""SQLAlchemy-based RDF store."""
import hashlib
import logging

import sqlalchemy
from rdflib import (
    BNode,
    Literal,
    URIRef
)
from rdflib.term import Statement, Variable
from rdflib.graph import Graph, QuotedGraph
from rdflib.namespace import RDF
from rdflib.plugins.stores.regexmatching import PYTHON_REGEX, REGEXTerm
from rdflib.store import CORRUPTED_STORE, VALID_STORE, NodePickler, Store
from six import text_type
from six.moves import reduce
from sqlalchemy import MetaData
from sqlalchemy.engine import reflection
from sqlalchemy.sql import expression, select

from rdflib_sqlalchemy.constants import (
    ASSERTED_LITERAL_PARTITION,
    ASSERTED_NON_TYPE_PARTITION,
    ASSERTED_TYPE_PARTITION,
    CONTEXT_SELECT,
    COUNT_SELECT,
    INTERNED_PREFIX,
    QUOTED_PARTITION,
    TRIPLE_SELECT_NO_ORDER,
)
from rdflib_sqlalchemy.tables import (
    create_asserted_statements_table,
    create_literal_statements_table,
    create_namespace_binds_table,
    create_quoted_statements_table,
    create_type_statements_table,
    get_table_names,
)
from rdflib_sqlalchemy.base import SQLGeneratorMixin
from rdflib_sqlalchemy.sql import union_select
from rdflib_sqlalchemy.statistics import StatisticsMixin
from rdflib_sqlalchemy.termutils import extract_triple


_logger = logging.getLogger(__name__)

Any = None


def generate_interned_id(identifier):
    return "{prefix}{identifier_hash}".format(
        prefix=INTERNED_PREFIX,
        identifier_hash=hashlib.sha1(identifier.encode("utf8")).hexdigest()[:10],
    )


class SQLAlchemy(Store, SQLGeneratorMixin, StatisticsMixin):
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

    def __init__(self, identifier=None, configuration=None, engine=None):
        """
        Initialisation.

        Args:
            identifier (rdflib.URIRef): URIRef of the Store. Defaults to CWD.
            engine (sqlalchemy.engine.Engine, optional): a `SQLAlchemy.engine.Engine` instance

        """
        self.identifier = identifier and identifier or "hardcoded"
        self.engine = engine

        # Use only the first 10 bytes of the digest
        self._interned_id = generate_interned_id(self.identifier)

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
        self._node_pickler = None

        self._create_table_definitions()

        # XXX For backward compatibility we still support getting the connection string in constructor
        # TODO: deprecate this once refactoring is more mature
        if configuration:
            self.open(configuration)

    def __repr__(self):
        """Readable serialisation."""
        quoted_table = self.tables["quoted_statements"]
        asserted_table = self.tables["asserted_statements"]
        asserted_type_table = self.tables["type_statements"]
        literal_table = self.tables["literal_statements"]

        selects = [
            (expression.alias(asserted_type_table, "typetable"), None, ASSERTED_TYPE_PARTITION),
            (expression.alias(quoted_table, "quoted"), None, QUOTED_PARTITION),
            (expression.alias(asserted_table, "asserted"), None, ASSERTED_NON_TYPE_PARTITION),
            (expression.alias(literal_table, "literal"), None, ASSERTED_LITERAL_PARTITION),
        ]
        q = union_select(selects, distinct=False, select_type=COUNT_SELECT)
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

        quotedContext = self.build_context_clause(context, quoted)
        assertedContext = self.build_context_clause(context, asserted)
        typeContext = self.build_context_clause(context, typetable)
        literalContext = self.build_context_clause(context, literal)

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
            q = union_select(selects, distinct=True, select_type=COUNT_SELECT)
        else:
            selects = [
                (typetable, typeContext,
                 ASSERTED_TYPE_PARTITION),
                (asserted, assertedContext,
                 ASSERTED_NON_TYPE_PARTITION),
                (literal, literalContext,
                 ASSERTED_LITERAL_PARTITION), ]
            q = union_select(selects, distinct=False, select_type=COUNT_SELECT)

        with self.engine.connect() as connection:
            res = connection.execute(q)
            rt = res.fetchall()
            return reduce(lambda x, y: x + y, [rtTuple[0] for rtTuple in rt])

    @property
    def table_names(self):
        return get_table_names(interned_id=self._interned_id)

    @property
    def node_pickler(self):
        if getattr(self, "_node_pickler", False) or self._node_pickler is None:
            self._node_pickler = np = NodePickler()
            np.register(self, "S")
            np.register(URIRef, "U")
            np.register(BNode, "B")
            np.register(Literal, "L")
            np.register(Graph, "G")
            np.register(QuotedGraph, "Q")
            np.register(Variable, "V")
            np.register(Statement, "s")
        return self._node_pickler

    def open(self, configuration, create=True):
        """
        Open the store specified by the configuration string.

        Args:
            create (bool): If create is True a store will be created if it does not already
                exist. If create is False and a store does not already exist
                an exception is raised. An exception is also raised if a store
                exists, but there is insufficient permissions to open the
                store.

        Returns:
            int: CORRUPTED_STORE (0) if database exists but is empty,
                 VALID_STORE (1) if database exists and tables are all there,
                 NO_STORE (-1) if nothing exists

        """
        # Close any existing engine connection
        self.close()

        self.engine = sqlalchemy.create_engine(configuration)
        with self.engine.connect():
            if create:
                self.create_all()

            ret_value = self._verify_store_exists()

        if ret_value != VALID_STORE and not create:
            raise RuntimeError("open() - create flag was set to False, but store was not created previously.")

        return ret_value

    def create_all(self):
        """Create all of the database tables (idempotent)."""
        self.metadata.create_all(self.engine)

    def close(self, commit_pending_transaction=False):
        """
        Close the current store engine connection if one is open.

        """
        if self.engine:
            self.engine.dispose()
        self.engine = None

    def destroy(self, configuration):
        """
        Delete all tables and stored data associated with the store.

        """
        if self.engine is None:
            self.engine = self.open(configuration, create=False)

        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                self.metadata.drop_all(self.engine)
                trans.commit()
            except Exception:
                _logger.exception("unable to drop table.")
                trans.rollback()

    # Triple Methods

    def add(self, triple, context=None, quoted=False):
        """Add a triple to the store of triples."""
        subject, predicate, obj = triple
        _, statement, params = self._get_build_command(
            (subject, predicate, obj),
            context, quoted,
        )

        with self.engine.connect() as connection:
            try:
                connection.execute(statement, params)
            except Exception:
                _logger.exception(
                    "Add failed with statement: %s, params: %s",
                    str(statement), repr(params)
                )
                raise

    def addN(self, quads):
        """Add a list of triples in quads form."""
        commands_dict = {}
        for subject, predicate, obj, context in quads:
            command_type, statement, params = self._get_build_command(
                (subject, predicate, obj),
                context,
                isinstance(context, QuotedGraph),
            )

            command_dict = commands_dict.setdefault(command_type, {})
            command_dict.setdefault("statement", statement)
            command_dict.setdefault("params", []).append(params)

        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                for command in commands_dict.values():
                    connection.execute(command["statement"], command["params"])
                trans.commit()
            except Exception:
                _logger.exception("AddN failed.")
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

                    if not self.STRONGLY_TYPED_TERMS or isinstance(obj, Literal):
                        # remove literal triple
                        clause = self.build_clause(literal_table, subject, predicate, obj, context)
                        connection.execute(literal_table.delete(clause))

                    for table in [quoted_table, asserted_table]:
                        # If asserted non rdf:type table and obj is Literal,
                        # don't do anything (already taken care of)
                        if table == asserted_table and isinstance(obj, Literal):
                            continue
                        else:
                            clause = self.build_clause(table, subject, predicate, obj, context)
                            connection.execute(table.delete(clause))

                if predicate == RDF.type or not predicate:
                    # Need to check rdf:type and quoted partitions (in addition
                    # perhaps)
                    clause = self.build_clause(asserted_type_table, subject, RDF.type, obj, context, True)
                    connection.execute(asserted_type_table.delete(clause))

                    clause = self.build_clause(quoted_table, subject, predicate, obj, context)
                    connection.execute(quoted_table.delete(clause))

                trans.commit()
            except Exception:
                _logger.exception("Removal failed.")
                trans.rollback()

    def triples(self, triple, context=None):
        """
        A generator over all the triples matching pattern.

        Pattern can be any objects for comparing against nodes in
        the store, for example, RegExLiteral, Date? DateRange?

        quoted table:                <id>_quoted_statements
        asserted rdf:type table:     <id>_type_statements
        asserted non rdf:type table: <id>_asserted_statements

        triple columns:
            subject, predicate, object, context, termComb, objLanguage, objDatatype
        class membership columns:
            member, klass, context, termComb

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
            clause = self.build_clause(typeTable, subject, RDF.type, obj, context, True)
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
                clause = self.build_clause(literal, subject, predicate, obj, context)
                selects.append((literal, clause, ASSERTED_LITERAL_PARTITION))

            if not isinstance(obj, Literal) \
                    and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                asserted = expression.alias(asserted_table, "asserted")
                clause = self.build_clause(asserted, subject, predicate, obj, context)
                selects.append((asserted, clause, ASSERTED_NON_TYPE_PARTITION))

            typeTable = expression.alias(asserted_type_table, "typetable")
            clause = self.build_clause(typeTable, subject, RDF.type, obj, context, True)
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
                clause = self.build_clause(literal, subject, predicate, obj, context)
                selects.append((literal, clause, ASSERTED_LITERAL_PARTITION))

            if not isinstance(obj, Literal) \
                    and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                    or not obj:
                asserted = expression.alias(asserted_table, "asserted")
                clause = self.build_clause(asserted, subject, predicate, obj, context)
                selects.append((asserted, clause, ASSERTED_NON_TYPE_PARTITION))

        if context is not None:
            quoted = expression.alias(quoted_table, "quoted")
            clause = self.build_clause(quoted, subject, predicate, obj, context)
            selects.append((quoted, clause, QUOTED_PARTITION))

        q = union_select(selects, select_type=TRIPLE_SELECT_NO_ORDER)
        with self.engine.connect() as connection:
            res = connection.execute(q)
            # TODO: False but it may have limitations on text column. Check
            # NOTE: SQLite does not support ORDER BY terms that aren't
            # integers, so the entire result set must be iterated in order
            # to be able to return a generator of contexts
            result = res.fetchall()
        tripleCoverage = {}

        for rt in result:
            id, s, p, o, (graphKlass, idKlass, graphId) = extract_triple(rt, self, context)
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

    def contexts(self, triple=None):
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
                clause = self.build_clause(typetable, subject, RDF.type, obj, Any, True)
                selects = [(typetable, clause, ASSERTED_TYPE_PARTITION), ]

            elif isinstance(predicate, REGEXTerm) \
                    and predicate.compiledExpr.match(RDF.type) \
                    or not predicate:
                # Select from quoted partition (if context is specified),
                # literal partition if (obj is Literal or None) and
                # asserted non rdf:type partition (if obj is URIRef
                # or None)
                clause = self.build_clause(typetable, subject, RDF.type, obj, Any, True)
                selects = [(typetable, clause, ASSERTED_TYPE_PARTITION), ]

                if (not self.STRONGLY_TYPED_TERMS or
                        isinstance(obj, Literal) or
                        not obj or
                        (self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm))):
                    clause = self.build_clause(literal, subject, predicate, obj)
                    selects.append(
                        (literal, clause, ASSERTED_LITERAL_PARTITION))
                if not isinstance(obj, Literal) \
                        and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                        or not obj:
                    clause = self.build_clause(asserted, subject, predicate, obj)
                    selects.append((asserted, clause, ASSERTED_NON_TYPE_PARTITION))

            elif predicate:
                # select from asserted non rdf:type partition (optionally),
                # quoted partition (if context is speciied), and literal
                # partition (optionally)
                selects = []
                if (not self.STRONGLY_TYPED_TERMS or
                        isinstance(obj, Literal) or not obj or (
                            self.STRONGLY_TYPED_TERMS and isinstance(obj, REGEXTerm))):
                    clause = self.build_clause(literal, subject, predicate, obj)
                    selects.append(
                        (literal, clause, ASSERTED_LITERAL_PARTITION))
                if not isinstance(obj, Literal) \
                        and not (isinstance(obj, REGEXTerm) and self.STRONGLY_TYPED_TERMS) \
                        or not obj:
                    clause = self.build_clause(asserted, subject, predicate, obj)
                    selects.append(
                        (asserted, clause, ASSERTED_NON_TYPE_PARTITION))

            clause = self.build_clause(quoted, subject, predicate, obj)
            selects.append((quoted, clause, QUOTED_PARTITION))
            q = union_select(selects, distinct=True, select_type=CONTEXT_SELECT)
        else:
            selects = [
                (typetable, None, ASSERTED_TYPE_PARTITION),
                (quoted, None, QUOTED_PARTITION),
                (asserted, None, ASSERTED_NON_TYPE_PARTITION),
                (literal, None, ASSERTED_LITERAL_PARTITION), ]
            q = union_select(selects, distinct=True, select_type=CONTEXT_SELECT)

        with self.engine.connect() as connection:
            res = connection.execute(q)
            rt = res.fetchall()
        for context in [rtTuple[0] for rtTuple in rt]:
            yield URIRef(context)

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
                _logger.exception("Namespace binding failed.")

    def prefix(self, namespace):
        """Prefix."""
        with self.engine.connect() as connection:
            nb_table = self.tables["namespace_binds"]
            namespace = text_type(namespace)
            s = select([nb_table.c.prefix]).where(nb_table.c.uri == namespace)
            res = connection.execute(s)
            rt = [rtTuple[0] for rtTuple in res.fetchall()]
            res.close()
            if rt and (rt[0] or rt[0] == ""):
                return rt[0]
        return None

    def namespace(self, prefix):
        res = None
        prefix_val = text_type(prefix)
        try:
            with self.engine.connect() as connection:
                nb_table = self.tables["namespace_binds"]
                s = select([nb_table.c.uri]).where(nb_table.c.prefix == prefix_val)
                res = connection.execute(s)
                rt = [rtTuple[0] for rtTuple in res.fetchall()]
                res.close()
                return rt and URIRef(rt[0]) or None
        except Exception:
            _logger.warning('exception in namespace retrieval', exc_info=True)
            return None

    def namespaces(self):
        with self.engine.connect() as connection:
            res = connection.execute(self.tables["namespace_binds"].select())
            for prefix, uri in res.fetchall():
                yield prefix, uri

    # Private methods

    def _create_table_definitions(self):
        self.metadata = MetaData()
        self.tables = {
            "asserted_statements": create_asserted_statements_table(self._interned_id, self.metadata),
            "type_statements": create_type_statements_table(self._interned_id, self.metadata),
            "literal_statements": create_literal_statements_table(self._interned_id, self.metadata),
            "quoted_statements": create_quoted_statements_table(self._interned_id, self.metadata),
            "namespace_binds": create_namespace_binds_table(self._interned_id, self.metadata),
        }

    def _get_build_command(self, triple, context=None, quoted=False):
        """
        Assemble the SQL Query text for adding an RDF triple to store.

        :param triple {tuple} - tuple of (subject, predicate, object) objects to add
        :param context - a `rdflib.URIRef` identifier for the graph namespace
        :param quoted {bool} - whether should treat as a quoted statement

        :returns {tuple} of (command_type, add_command, params):
            command_type: which kind of statement it is: literal, type, other
            statement: the literal SQL statement to execute (with unbound variables)
            params: the parameters for the SQL statement (e.g the variables to bind)

        """
        subject, predicate, obj = triple
        command_type = None
        if quoted or predicate != RDF.type:
            # Quoted statement or non rdf:type predicate
            # check if object is a literal
            if isinstance(obj, Literal):
                statement, params = self._build_literal_triple_sql_command(
                    subject,
                    predicate,
                    obj,
                    context,
                )
                command_type = "literal"
            else:
                statement, params = self._build_triple_sql_command(
                    subject,
                    predicate,
                    obj,
                    context,
                    quoted,
                )
                command_type = "other"
        elif predicate == RDF.type:
            # asserted rdf:type statement
            statement, params = self._build_type_sql_command(
                subject,
                obj,
                context,
            )
            command_type = "type"
        return command_type, statement, params

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
                    clause = self.build_context_clause(identifier, table)
                    connection.execute(table.delete(clause))
                trans.commit()
            except Exception:
                _logger.exception("Context removal failed.")
                trans.rollback()

    def _verify_store_exists(self):
        """
        Verify store (e.g. all tables) exist.

        """

        inspector = reflection.Inspector.from_engine(self.engine)
        existing_table_names = inspector.get_table_names()
        for table_name in self.table_names:
            if table_name not in existing_table_names:
                _logger.critical("create_all() - table %s Doesn't exist!", table_name)
                # The database exists, but one of the tables doesn't exist
                return CORRUPTED_STORE

        return VALID_STORE
