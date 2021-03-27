from sqlalchemy import Column, Table, Index, types

from rdflib_sqlalchemy.types import TermType


MYSQL_MAX_INDEX_LENGTH = 200

TABLE_NAME_TEMPLATES = [
    "{interned_id}_asserted_statements",
    "{interned_id}_literal_statements",
    "{interned_id}_namespace_binds",
    "{interned_id}_quoted_statements",
    "{interned_id}_type_statements",
]


def get_table_names(interned_id):
    return [
        table_name_template.format(interned_id=interned_id)
        for table_name_template in TABLE_NAME_TEMPLATES
    ]


def create_asserted_statements_table(interned_id, metadata):
    return Table(
        "{interned_id}_asserted_statements".format(interned_id=interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("subject", TermType, nullable=False),
        Column("predicate", TermType, nullable=False),
        Column("object", TermType, nullable=False),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Index(
            "{interned_id}_A_s_index".format(interned_id=interned_id),
            "subject",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_A_p_index".format(interned_id=interned_id),
            "predicate",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_A_o_index".format(interned_id=interned_id),
            "object",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_A_c_index".format(interned_id=interned_id),
            "context",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_A_termComb_index".format(interned_id=interned_id),
            "termComb",
        ),
        Index(
            "{interned_id}_asserted_spoc_key".format(interned_id=interned_id),
            "subject",
            "predicate",
            "object",
            "context",
            unique=True,
            mysql_length=191,
        ),
    )


def create_type_statements_table(interned_id, metadata):
    return Table(
        "{interned_id}_type_statements".format(interned_id=interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("member", TermType, nullable=False),
        Column("klass", TermType, nullable=False),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Index(
            "{interned_id}_member_index".format(interned_id=interned_id),
            "member",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_klass_index".format(interned_id=interned_id),
            "klass",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_c_index".format(interned_id=interned_id),
            "context",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_T_termComb_index".format(interned_id=interned_id),
            "termComb",
        ),
        Index(
            "{interned_id}_type_mkc_key".format(interned_id=interned_id),
            "member",
            "klass",
            "context",
            unique=True,
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
    )


def create_literal_statements_table(interned_id, metadata):
    return Table(
        "{interned_id}_literal_statements".format(interned_id=interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("subject", TermType, nullable=False),
        Column("predicate", TermType, nullable=False),
        Column("object", TermType),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Column("objlanguage", types.String(255), key="objLanguage"),
        Column("objdatatype", types.String(255), key="objDatatype"),
        Index(
            "{interned_id}_L_s_index".format(interned_id=interned_id),
            "subject",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_L_p_index".format(interned_id=interned_id),
            "predicate",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_L_c_index".format(interned_id=interned_id),
            "context",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_L_termComb_index".format(interned_id=interned_id),
            "termComb",
        ),
        Index(
            "{interned_id}_literal_spoc_key".format(interned_id=interned_id),
            "subject",
            "predicate",
            "object",
            "objLanguage",
            "context",
            unique=True,
            mysql_length=153,
        ),
    )


def create_quoted_statements_table(interned_id, metadata):
    return Table(
        "{interned_id}_quoted_statements".format(interned_id=interned_id),
        metadata,
        Column("id", types.Integer, nullable=False, primary_key=True),
        Column("subject", TermType, nullable=False),
        Column("predicate", TermType, nullable=False),
        Column("object", TermType),
        Column("context", TermType, nullable=False),
        Column("termcomb", types.Integer, nullable=False, key="termComb"),
        Column("objlanguage", types.String(255), key="objLanguage"),
        Column("objdatatype", types.String(255), key="objDatatype"),
        Index(
            "{interned_id}_Q_s_index".format(interned_id=interned_id),
            "subject",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_Q_p_index".format(interned_id=interned_id),
            "predicate",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_Q_o_index".format(interned_id=interned_id),
            "object",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_Q_c_index".format(interned_id=interned_id),
            "context",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        ),
        Index(
            "{interned_id}_Q_termComb_index".format(interned_id=interned_id),
            "termComb",
        ),
        Index(
            "{interned_id}_quoted_spoc_key".format(interned_id=interned_id),
            "subject",
            "predicate",
            "object",
            "objLanguage",
            "context",
            unique=True,
            mysql_length=153,
        ),
    )


def create_namespace_binds_table(interned_id, metadata):
    return Table(
        "{interned_id}_namespace_binds".format(interned_id=interned_id),
        metadata,
        Column("prefix", types.String(20), unique=True, nullable=False, primary_key=True),
        Column("uri", types.Text),
        Index(
            "{interned_id}_uri_index".format(interned_id=interned_id),
            "uri",
            mysql_length=MYSQL_MAX_INDEX_LENGTH,
        )
    )
