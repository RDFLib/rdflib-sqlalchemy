"""Statistical summary of store statements mixin"""
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import func


def get_group_by_count(session, group_by_column):
    """
    Construct SQL query to get counts for distinct values using GROUP BY.

    Args:
        session (~sqlalchemy.orm.session.Session): session to query in
        group_by_column (~sqlalchemy.schema.Column): column to group by

    Returns:
        dict: dictionary mapping from value to count
    """
    return dict(
        session.query(
            group_by_column,
            func.count(group_by_column)
        ).group_by(group_by_column).all()
    )


class StatisticsMixin:
    ''' Has methods for statistics on stores '''
    def statistics(self, asserted_statements=True, literals=True, types=True):
        """Store statistics."""
        statistics = {
            "store": dict(total_num_statements=len(self)),
        }

        with self.engine.connect() as connection:
            session = Session(bind=connection)
            if asserted_statements:
                table = self.tables["asserted_statements"]
                group_by_column = table.c.predicate
                statistics["asserted_statements"] = get_group_by_count(session, group_by_column)
            if literals:
                table = self.tables["literal_statements"]
                group_by_column = table.c.predicate
                statistics["literals"] = get_group_by_count(session, group_by_column)
            if types:
                table = self.tables["type_statements"]
                group_by_column = table.c.klass
                statistics["types"] = get_group_by_count(session, group_by_column)

        return statistics
