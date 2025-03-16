from rdflib.graph import Graph, QuotedGraph
from rdflib.term import Node
from sqlalchemy import types


class TermType(types.TypeDecorator):
    """Term typology."""

    impl = types.Text()
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """Process bound parameters."""
        if isinstance(value, (QuotedGraph, Graph)):
            return str(value.identifier)
        elif isinstance(value, Node):
            return str(value)
        else:
            return value
