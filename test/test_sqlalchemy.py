import unittest

try:
    from unittest.mock import patch, MagicMock
except ImportError:
    from mock import patch, MagicMock

import six

from rdflib import (
    ConjunctiveGraph,
    Literal,
    URIRef,
    plugin
)
from rdflib.store import Store

from rdflib_sqlalchemy import registerplugins
from sqlalchemy.sql.selectable import Select


michel = URIRef(u"michel")
likes = URIRef(u"likes")
pizza = URIRef(u"pizza")


class mock_cursor():
    def execute(x):
        raise Exception("Forced exception")


class ConfigTest(unittest.TestCase):
    '''
    Test configuration with a dict
    '''

    def setUp(self):
        self.store = plugin.get("SQLAlchemy", Store)()
        self.graph = ConjunctiveGraph(self.store)

    def tearDown(self):
        self.graph.close()

    def test_success(self):
        with patch('rdflib_sqlalchemy.store.sqlalchemy') as p:
            self.graph.open({'url': 'sqlite://', 'random_key': 'something'}, create=True)
            p.create_engine.assert_called_with('sqlite://', random_key='something')

    def test_no_url(self):
        with patch('rdflib_sqlalchemy.store.sqlalchemy'):
            with six.assertRaisesRegex(self, Exception, '.*url.*'):
                self.graph.open({'random_key': 'something'}, create=True)


class SQLATestCase(unittest.TestCase):
    identifier = URIRef("rdflib_test")
    dburi = Literal("sqlite://")

    def setUp(self):
        self.store = plugin.get(
            "SQLAlchemy", Store)(identifier=self.identifier, configuration=self.dburi)
        self.graph = ConjunctiveGraph(self.store, identifier=self.identifier)
        self.graph.open(self.dburi, create=True)

    def tearDown(self):
        self.graph.destroy(self.dburi)
        self.graph.close()

    def test_registerplugins(self):
        # I doubt this is quite right for a fresh pip installation,
        # this test is mainly here to fill a coverage gap.
        registerplugins()
        self.assertIsNotNone(plugin.get("SQLAlchemy", Store))
        p = plugin._plugins
        self.assertIn(("SQLAlchemy", Store), p)
        del p[("SQLAlchemy", Store)]
        plugin._plugins = p
        registerplugins()
        self.assertIn(("SQLAlchemy", Store), p)

    def test_namespaces(self):
        self.assertNotEqual(list(self.graph.namespaces()), [])

    def test_contexts_without_triple(self):
        self.assertEqual(list(self.graph.contexts()), [])

    def test_contexts_result(self):
        ctx_id = URIRef('http://example.org/context')
        g = self.graph.get_context(ctx_id)
        g.add((michel, likes, pizza))
        actual = list(self.store.contexts())
        self.assertEqual(actual[0], ctx_id)

    def test_contexts_with_triple(self):
        statemnt = (michel, likes, pizza)
        self.assertEqual(list(self.graph.contexts(triple=statemnt)), [])

    def test__len(self):
        self.assertEqual(self.store.__len__(), 0)

    def test__remove_context(self):
        ctx_id = URIRef('http://example.org/context')
        g = self.graph.get_context(ctx_id)
        g.add((michel, likes, pizza))
        self.store._remove_context(g)
        self.assertEqual(list(self.store.contexts()), [])

    def test_triples_choices(self):
        # Create a mock for the sqlalchemy engine so we can capture the arguments
        p = MagicMock(name='engine')
        self.store.engine = p

        # Set this so we're not including selects for both asserted and literal tables for
        # a choice
        self.store.STRONGLY_TYPED_TERMS = True
        # Set the grouping of terms
        self.store.max_terms_per_where = 2
        # force execution of the generator
        for x in self.store.triples_choices((None, likes, [michel, pizza, likes])):
            pass
        args = p.connect().__enter__().execute.call_args[0]
        children = args[0].get_children(column_collections=False)
        # Expect two selects: one for the first two choices plus one for the last one
        self.assertEqual(sum(1 for c in children if isinstance(c, Select)), 2)


if __name__ == "__main__":
    unittest.main()
