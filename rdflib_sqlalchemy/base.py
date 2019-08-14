"""Base classes for the store."""
from rdflib import Literal
from rdflib.graph import Graph, QuotedGraph
from rdflib.plugins.stores.regexmatching import REGEXTerm
from sqlalchemy.sql import expression

from rdflib_sqlalchemy.termutils import (
    type_to_term_combination,
    statement_to_term_combination,
)


class SQLGeneratorMixin(object):
    """SQL statement generator mixin for the SQLAlchemy store."""

    def _build_type_sql_command(self, member, klass, context):
        """Build an insert command for a type table."""
        # columns: member,klass,context
        rt = self.tables["type_statements"].insert()
        return rt, {
            "member": member,
            "klass": klass,
            "context": context.identifier,
            "termComb": int(type_to_term_combination(member, klass, context))
        }

    def _build_literal_triple_sql_command(self, subject, predicate, obj, context):
        """
        Build an insert command for literal triples.

        These triples correspond to RDF statements where the object is a Literal,
        e.g. `rdflib.Literal`.

        """
        triple_pattern = int(
            statement_to_term_combination(subject, predicate, obj, context)
        )

        command = self.tables["literal_statements"].insert()
        values = {
            "subject": subject,
            "predicate": predicate,
            "object": obj,
            "context": context.identifier,
            "termComb": triple_pattern,
            "objLanguage": isinstance(obj, Literal) and obj.language or None,
            "objDatatype": isinstance(obj, Literal) and obj.datatype or None,
        }
        return command, values

    def _build_triple_sql_command(self, subject, predicate, obj, context, quoted):
        """
        Build an insert command for regular triple table.

        """
        stmt_table = (quoted and
                      self.tables["quoted_statements"] or
                      self.tables["asserted_statements"])

        triple_pattern = statement_to_term_combination(
            subject,
            predicate,
            obj,
            context,
        )
        command = stmt_table.insert()

        if quoted:
            params = {
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "context": context.identifier,
                "termComb": triple_pattern,
                "objLanguage": isinstance(obj, Literal) and obj.language or None,
                "objDatatype": isinstance(obj, Literal) and obj.datatype or None
            }
        else:
            params = {
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "context": context.identifier,
                "termComb": triple_pattern,
            }
        return command, params

    def build_clause(self, table, subject, predicate, obj, context=None, typeTable=False):
        """Build WHERE clauses for the supplied terms and, context."""
        if typeTable:
            clauseList = [
                self.build_type_member_clause(subject, table),
                self.build_type_class_clause(obj, table),
                self.build_context_clause(context, table)
            ]
        else:
            clauseList = [
                self.build_subject_clause(subject, table),
                self.build_predicate_clause(predicate, table),
                self.build_object_clause(obj, table),
                self.build_context_clause(context, table),
                self.build_literal_datatype_clause(obj, table),
                self.build_literal_language_clause(obj, table)
            ]

        clauseList = [clause for clause in clauseList if clause is not None]
        if clauseList:
            return expression.and_(*clauseList)
        else:
            return None

    def build_literal_datatype_clause(self, obj, table):
        """Build Literal and datatype clause."""
        if isinstance(obj, Literal) and obj.datatype is not None:
            return table.c.objDatatype == obj.datatype
        else:
            return None

    def build_literal_language_clause(self, obj, table):
        """Build Literal and language clause."""
        if isinstance(obj, Literal) and obj.language is not None:
            return table.c.objLanguage == obj.language
        else:
            return None

    # Where Clause  utility Functions
    # The predicate and object clause builders are modified in order
    # to optimize subjects and objects utility functions which can
    # take lists as their last argument (object, predicate - respectively)

    def build_subject_clause(self, subject, table):
        """Build Subject clause."""
        if isinstance(subject, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.subject.op("REGEXP")(subject)
        elif isinstance(subject, list):
            # clauseStrings = [] --- unused
            return expression.or_(
                *[self.build_subject_clause(s, table) for s in subject if s])
        elif isinstance(subject, (QuotedGraph, Graph)):
            return table.c.subject == subject.identifier
        elif subject is not None:
            return table.c.subject == subject
        else:
            return None

    def build_predicate_clause(self, predicate, table):
        """
        Build Predicate clause.

        Capable of taking a list of predicates as well, in which case
        subclauses are joined with 'OR'.

        """
        if isinstance(predicate, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.predicate.op("REGEXP")(predicate)
        elif isinstance(predicate, list):
            return expression.or_(
                *[self.build_predicate_clause(p, table) for p in predicate if p])
        elif predicate is not None:
            return table.c.predicate == predicate
        else:
            return None

    def build_object_clause(self, obj, table):
        """
        Build Object clause.

        Capable of taking a list of objects as well, in which case subclauses
        are joined with 'OR'.

        """
        if isinstance(obj, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.object.op("REGEXP")(obj)
        elif isinstance(obj, list):
            return expression.or_(
                *[self.build_object_clause(o, table) for o in obj if o])
        elif isinstance(obj, (QuotedGraph, Graph)):
            return table.c.object == obj.identifier
        elif obj is not None:
            return table.c.object == obj
        else:
            return None

    def build_context_clause(self, context, table):
        """Build Context clause."""
        if isinstance(context, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.context.op("regexp")(context.identifier)
        elif context is not None and context.identifier is not None:
            return table.c.context == context.identifier
        else:
            return None

    def build_type_member_clause(self, subject, table):
        """Build Type Member clause."""
        if isinstance(subject, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.member.op("regexp")(subject)
        elif isinstance(subject, list):
            return expression.or_(
                *[self.build_type_member_clause(s, table) for s in subject if s])
        elif subject is not None:
            return table.c.member == subject
        else:
            return None

    def build_type_class_clause(self, obj, table):
        """Build Type Class clause."""
        if isinstance(obj, REGEXTerm):
            # TODO: this work only in mysql. Must adapt for postgres and sqlite
            return table.c.klass.op("regexp")(obj)
        elif isinstance(obj, list):
            return expression.or_(
                *[self.build_type_class_clause(o, table) for o in obj if o])
        elif obj is not None:
            return obj and table.c.klass == obj
        else:
            return None
