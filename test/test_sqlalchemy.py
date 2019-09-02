import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import six

from rdflib import (
    ConjunctiveGraph,
    Literal,
    URIRef,
    plugin
)
from rdflib.store import Store

from rdflib_sqlalchemy import registerplugins


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
            "SQLAlchemy", Store)(identifier=self.identifier)
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

    def test_contexts_with_triple(self):
        statemnt = (michel, likes, pizza)
        self.assertEqual(list(self.graph.contexts(triple=statemnt)), [])

    def test__len(self):
        self.assertEqual(self.store.__len__(), 0)

    def test__remove_context(self):
        self.store._remove_context(self.identifier)


if __name__ == "__main__":
    unittest.main()
