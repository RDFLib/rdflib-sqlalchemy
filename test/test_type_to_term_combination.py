import unittest

from rdflib import Literal, SDO, Graph

from rdflib_sqlalchemy.termutils import type_to_term_combination


class TypeToTermCombinationTestCase(unittest.TestCase):
    """Test the type_to_term_combination function."""

    def test_luuu(self):
        """Literal is not a valid subject for a triple."""
        with self.assertRaises(ValueError):
            type_to_term_combination(
                member=Literal('https://example.org'),
                klass=SDO.WebSite,
                context=Graph(identifier='local://test-graph/'),
            )
