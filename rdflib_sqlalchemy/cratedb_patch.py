import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.base import RESERVED_WORDS as POSTGRESQL_RESERVED_WORDS


def cratedb_patch_dialect():
    try:
        from crate.client.sqlalchemy import CrateDialect
        from crate.client.sqlalchemy.compiler import CrateDDLCompiler
    except ImportError:
        return

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True, **kw
    ):
        return "SELECT 1;"

    CrateDDLCompiler.visit_create_index = visit_create_index
    CrateDialect.preparer = CrateIdentifierPreparer


def cratedb_polyfill_refresh_after_dml_engine(engine: sa.engine.Engine):
    def receive_after_execute(
        conn: sa.engine.Connection, clauseelement, multiparams, params, execution_options, result
    ):
        """
        Run a `REFRESH TABLE ...` command after each DML operation (INSERT, UPDATE, DELETE).
        """

        if isinstance(clauseelement, (sa.sql.Insert, sa.sql.Update, sa.sql.Delete)):
            if not isinstance(clauseelement.table, sa.sql.Join):
                full_table_name = f'"{clauseelement.table.name}"'
                if clauseelement.table.schema is not None:
                    full_table_name = f'"{clauseelement.table.schema}".' + full_table_name
                conn.execute(sa.text(f'REFRESH TABLE {full_table_name};'))

    sa.event.listen(engine, "after_execute", receive_after_execute)


RESERVED_WORDS = set(list(POSTGRESQL_RESERVED_WORDS) + ["object"])


class CrateIdentifierPreparer(sa.sql.compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS

    def _unquote_identifier(self, value):
        if value[0] == self.initial_quote:
            value = value[1:-1].replace(
                self.escape_to_quote, self.escape_quote
            )
        return value

    def format_type(self, type_, use_schema=True):
        if not type_.name:
            raise sa.exc.CompileError("PostgreSQL ENUM type requires a name.")

        name = self.quote(type_.name)
        effective_schema = self.schema_for_object(type_)

        if (
            not self.omit_schema
            and use_schema
            and effective_schema is not None
        ):
            name = self.quote_schema(effective_schema) + "." + name
        return name
