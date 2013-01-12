#import os
import logging
import context_case
import graph_case
from rdflib import Literal
from nose.exc import SkipTest
raise SkipTest("SQLAlchemyBase not yet fir to test")
_logger = logging.getLogger(__name__)

# sqlalchemy_url = Literal("mysql://username:password@hostname:port/database-name?other-parameter")
# sqlalchemy_url = Literal("mysql+mysqldb://user:password@hostname:port/database?charset=utf8")
# sqlalchemy_url = Literal('postgresql+psycopg2://user:pasword@hostname:port/database')
# sqlalchemy_url = Literal('sqlite:////absolute/path/to/foo.db')
# sqlalchemy_url = Literal("sqlite:///%(here)s/development.sqlite" % {"here": os.getcwd()})
# #sqlalchemy_url = Literal('sqlite://')
# #sqlalchemy_url = Literal("mysql://gjh:50uthf0rk@localhost:3306/test")
sqlalchemy_url = Literal('sqlite://')


class SQLAlchemyBaseGraphTestCase(graph_case.GraphTestCase):
    storetest = True
    storename = "SQLAlchemyBase"
    uri = sqlalchemy_url

    def setUp(self):
        graph_case.GraphTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        self.create = False
        graph_case.GraphTestCase.tearDown(self, uri=self.uri)

    def testStatementNode(self):
        raise SkipTest("RDF Statements not supported in FOPL model.")


class SQLAlchemyBaseContextTestCase(context_case.ContextTestCase):
    storetest = True
    storename = "SQLAlchemyBase"
    uri = sqlalchemy_url

    def setUp(self):
        context_case.ContextTestCase.setUp(
            self, uri=self.uri, storename=self.storename)

    def tearDown(self):
        self.create = False
        context_case.ContextTestCase.tearDown(self, uri=self.uri)

SQLAlchemyBaseGraphTestCase.storetest = True
SQLAlchemyBaseContextTestCase.storetest = True
