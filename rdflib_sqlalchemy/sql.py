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


def union_select(selectComponents, distinct=False, select_type=TRIPLE_SELECT):
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

        if select_type == COUNT_SELECT:
            selectClause = table.count(whereClause)
        elif select_type == CONTEXT_SELECT:
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
                 expression.literal_column("NULL").label("objdatatype")],
                whereClause)
        elif tableType == ASSERTED_NON_TYPE_PARTITION:
            selectClause = expression.select(
                [c for c in table.columns] +
                [expression.literal_column("NULL").label("objlanguage"),
                 expression.literal_column("NULL").label("objdatatype")],
                whereClause,
                from_obj=[table])

        selects.append(selectClause)

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
