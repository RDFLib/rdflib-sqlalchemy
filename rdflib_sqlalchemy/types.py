from rdflib.graph import Graph, QuotedGraph
from rdflib.term import Node
from six import text_type
from sqlalchemy import types


class TermType(types.TypeDecorator):
    """Term typology."""

    impl = types.Text()
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """Process bound parameters."""
        if isinstance(value, (QuotedGraph, Graph)):
            return text_type(value.identifier)
        elif isinstance(value, Node):
            return text_type(value)
        else:
            return value
