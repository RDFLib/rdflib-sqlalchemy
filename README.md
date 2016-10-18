RDFLib-SQLAlchemy
=================

NOTE: This is a fork of the original [rdflib-sqlalchemy](https://github.com/rdflib/rdflib-sqlalchemy). To install this fork instead of the original version with pip, use:

	pip install rdflib-sqlalchemy-redux

---

A SQLAlchemy-backed, formula-aware RDFLib Store. It stores its triples
in the following partitions:

- Asserted non rdf:type statements.
- Asserted rdf:type statements (in a table which models Class membership). The motivation for this partition is primarily query speed and scalability as most graphs will always have more rdf:type statements than others.
- All Quoted statements.

In addition, it persists namespace mappings in a separate table.

Back-end persistence
--------------------

Back-end persistence is provided by SQLAlchemy.

Tested dialects are:

- SQLite, using the built-in Python driver or, for Python 2.5, pysqlite
- MySQL, using the MySQLdb-python driver or, for Python 3, mysql-connector
- PostgreSQL, using the psycopg2 driver or the pg8000 driver.

pysqlite: https://pypi.python.org/pypi/pysqlite

MySQLdb-python: https://pypi.python.org/pypi/MySQL-python

mysql-connector: http://dev.mysql.com/doc/connector-python/en/connector-python.html

psycopg2: https://pypi.python.org/pypi/psycopg2

pg8000: https://pypi.python.org/pypi/pg8000

Development
===========
Github repository: https://github.com/RDFLib/rdflib-sqlalchemy

Continuous integration: https://travis-ci.org/RDFLib/rdflib-sqlalchemy/

![Travis CI](https://travis-ci.org/globality-corp/rdflib-sqlalchemy.png?branch=develop)
![PyPI](https://img.shields.io/pypi/v/rdflib-sqlalchemy-redux.svg)
![PyPI](https://img.shields.io/pypi/status/rdflib-sqlalchemy-redux.svg)
![PyPI](https://img.shields.io/pypi/dw/rdflib-sqlalchemy-redux.svg)

![PyPI](https://img.shields.io/pypi/pyversions/rdflib-sqlalchemy-redux.svg)
![PyPI](https://img.shields.io/pypi/l/rdflib-sqlalchemy-redux.svg)
![PyPI](https://img.shields.io/pypi/wheel/rdflib-sqlalchemy-redux.svg)
![PyPI](https://img.shields.io/pypi/format/rdflib-sqlalchemy-redux.svg)


An illustrative unit test:
==========================

```python

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
```

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
    dburi = Literal('postgresql+pg8000://user:pasword@hostname:port/database')
    dburi = Literal('sqlite:////absolute/path/to/foo.db')
    dburi = Literal("sqlite:///%(here)s/development.sqlite" % {"here": os.getcwd()})
    dburi = Literal('sqlite://') # In-memory
