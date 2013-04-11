RDFLib-SQLAlchemy
=================

A SQLAlchemy-backed, formula-aware RDFLib Store. It stores its triples
in the following partitions:

- Asserted non rdf:type statements
- Asserted rdf:type statements (in a table which models Class membership)
The motivation for this partition is primarily query speed and
scalability as most graphs will always have more rdf:type statements than others
- All Quoted statements

In addition, it persists namespace mappings in a separate table.

Development
===========
Github repository: https://github.com/RDFLib/rdflib-sqlalchemy

Travis-CI build status: [![Build Status](https://travis-ci.org/RDFLib/rdflib-sqlalchemy.png?branch=master)](https://travis-ci.org/RDFLib/rdflib-sqlalchemy)

An illustrative unit test:
==========================

.. code-block:: python

    import unittest
    from rdflib import plugin, Graph, Literal, URIRef
    from rdflib.store import Store


    class SQLASQLiteGraphTestCase(unittest.TestCase):
        ident = URIRef("rdflib_test")
        uri = Literal("sqlite://")

        def setUp(self):
            store = plugin.get("SQLAlchemy", Store)(identifier=self.ident)
            self.graph = Graph(store, identifier=self.ident)
            self.graph.open(self.uri, create=True)

        def tearDown(self):
            self.graph.destroy(self.uri)
            try:
                self.graph.close()
            except:
                pass

        def test01(self):
            self.assert_(self.graph is not None)
            print(self.graph)

    if __name__ == '__main__':
        unittest.main()

Running the tests
=================

This is slightly baroque because of the test matrix (PostgreSQL|MySQL|SQLite
x Python2.5|2.6|2.7|3.2) ...

Using nose::

    DB='pgsql' DBURI='postgresql+psycopg2://user:password@host/dbname' nosetests

Using tox::

    DB='pgsql' DBURI='postgresql+psycopg2://user:password@host/dbname' tox -e py32

DB variants are 'pgsql', 'mysql' and 'sqlite' 

Sample DBURI values::

    dburi = Literal("mysql://username:password@hostname:port/database-name?other-parameter")
    dburi = Literal("mysql+mysqldb://user:password@hostname:port/database?charset=utf8")
    dburi = Literal('postgresql+psycopg2://user:pasword@hostname:port/database')
    dburi = Literal('sqlite:////absolute/path/to/foo.db')
    dburi = Literal("sqlite:///%(here)s/development.sqlite" % {"here": os.getcwd()})
    dburi = Literal('sqlite://') # In-memory
