from rdflib.graph import Graph, QuotedGraph
from rdflib.term import Node
from six import text_type
from sqlalchemy import Column, Table, Index, types


MYSQL_MAX_INDEX_LENGTH = 200

TABLE_NAME_TEMPLATES = [
    "{}_asserted_statements",
    "{}_type_statements",
    "{}_quoted_statements",
    "{}_namespace_binds",
    "{}_literal_statements"
]


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


def create_asserted_statements_table(interned_id, metadata):
    return Table(
        "{}_asserted_statements".format(interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("subject", TermType, nullable=False),
        Column("predicate", TermType, nullable=False),
        Column("object", TermType, nullable=False),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Index("{}_A_termComb_index".format(interned_id), "termComb"),
        Index("{}_A_s_index".format(interned_id), "subject", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_A_p_index".format(interned_id), "predicate", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_A_o_index".format(interned_id), "object", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_A_c_index".format(interned_id), "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)
    )


def create_type_statements_table(interned_id, metadata):
    return Table(
        "{}_type_statements".format(interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("member", TermType, nullable=False),
        Column("klass", TermType, nullable=False),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Index("{}_T_termComb_index".format(interned_id), "termComb"),
        Index("{}_member_index".format(interned_id), "member", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_klass_index".format(interned_id), "klass", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_c_index".format(interned_id), "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)
    )


def create_literal_statements_table(interned_id, metadata):
    return Table(
        "{}_literal_statements".format(interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("subject", TermType, nullable=False),
        Column("predicate", TermType, nullable=False),
        Column("object", TermType),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Column("objlanguage", types.String(255), key="objLanguage"),
        Column("objdatatype", types.String(255), key="objDatatype"),
        Index("{}_L_termComb_index".format(interned_id), "termComb"),
        Index("{}_L_s_index".format(interned_id), "subject", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_L_p_index".format(interned_id), "predicate", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_L_c_index".format(interned_id), "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)
    )


def create_quoted_statements_table(interned_id, metadata):
    return Table(
        "{}_quoted_statements".format(interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("subject", TermType, nullable=False),
        Column("predicate", TermType, nullable=False),
        Column("object", TermType),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Column("objlanguage", types.String(255), key="objLanguage"),
        Column("objdatatype", types.String(255), key="objDatatype"),
        Index("{}_Q_termComb_index".format(interned_id), "termComb"),
        Index("{}_Q_s_index".format(interned_id), "subject", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_Q_p_index".format(interned_id), "predicate", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_Q_o_index".format(interned_id), "object", mysql_length=MYSQL_MAX_INDEX_LENGTH),
        Index("{}_Q_c_index".format(interned_id), "context", mysql_length=MYSQL_MAX_INDEX_LENGTH)
    )


def create_namespace_binds_table(interned_id, metadata):
    return Table(
        "{}_namespace_binds".format(interned_id),
        metadata,
        Column("prefix", types.String(20), unique=True, nullable=False, primary_key=True),
        Column("uri", types.Text),
        Index("{}_uri_index".format(interned_id), "uri", mysql_length=MYSQL_MAX_INDEX_LENGTH)
    )
