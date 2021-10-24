import unittest

from rdflib import plugin
from rdflib.graph import Graph, Store


class TestOpen(unittest.TestCase):
    def test_open_corrupted(self):
        store = plugin.get('SQLAlchemy', Store)(identifier='open_test')
        self.graph = Graph(store, identifier='open_test')
        with self.assertRaises(RuntimeError):
            self.graph.open("sqlite:///test/corrupted.sqlite", create=False)
