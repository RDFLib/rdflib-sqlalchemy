from rdflib.namespace import RDF
from six import text_type
from sqlalchemy.sql import expression

from rdflib_sqlalchemy.constants import (
    ASSERTED_TYPE_PARTITION,
    ASSERTED_NON_TYPE_PARTITION,
    CONTEXT_SELECT,
    COUNT_SELECT,
    FULL_TRIPLE_PARTITIONS,
    TRIPLE_SELECT,
)


def query_analysis(query, store, connection):
    """
    Helper function.

    For executing EXPLAIN on all dispatched SQL statements -
    for the pupose of analyzing index usage.

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


def union_select(select_components, distinct=False, select_type=TRIPLE_SELECT):
    """
    Helper function for building union all select statement.

    Terms:
        u - uri refs
        v - variables
        b - bnodes
        l - literal
        f - formula

    :param select_components - iterable of (table_name, where_clause_string, table_type) tuples

    """
    selects = []
    for table, whereClause, tableType in select_components:

        if select_type == COUNT_SELECT:
            select_clause = table.count(whereClause)
        elif select_type == CONTEXT_SELECT:
            select_clause = expression.select([table.c.context], whereClause)
        elif tableType in FULL_TRIPLE_PARTITIONS:
            select_clause = table.select(whereClause)
        elif tableType == ASSERTED_TYPE_PARTITION:
            select_clause = expression.select(
                [table.c.id.label("id"),
                 table.c.member.label("subject"),
                 expression.literal(text_type(RDF.type)).label("predicate"),
                 table.c.klass.label("object"),
                 table.c.context.label("context"),
                 table.c.termComb.label("termcomb"),
                 expression.literal_column("NULL").label("objlanguage"),
                 expression.literal_column("NULL").label("objdatatype")],
                whereClause)
        elif tableType == ASSERTED_NON_TYPE_PARTITION:
            select_clause = expression.select(
                [c for c in table.columns] +
                [expression.literal_column("NULL").label("objlanguage"),
                 expression.literal_column("NULL").label("objdatatype")],
                whereClause,
                from_obj=[table])

        selects.append(select_clause)

    order_statement = []
    if select_type == TRIPLE_SELECT:
        order_statement = [
            expression.literal_column("subject"),
            expression.literal_column("predicate"),
            expression.literal_column("object"),
        ]
    if distinct:
        return expression.union(*selects, **{"order_by": order_statement})
    else:
        return expression.union_all(*selects, **{"order_by": order_statement})
