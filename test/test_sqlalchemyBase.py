# import os
# import logging
# import test_context
# import test_graph
# from nose.exc import SkipTest
# from rdflib import Literal

# _logger = logging.getLogger(__name__)

# sqlalchemy_url = Literal("mysql://username:password@hostname:port/database-name?other-parameter")
# sqlalchemy_url = Literal("mysql+mysqldb://user:password@hostname:port/database?charset=utf8")
# sqlalchemy_url = Literal('postgresql+psycopg2://user:pasword@hostname:port/database')
# sqlalchemy_url = Literal('sqlite:////absolute/path/to/foo.db')
# sqlalchemy_url = Literal("sqlite:///%(here)s/development.sqlite" % {"here": os.getcwd()})
# #sqlalchemy_url = Literal('sqlite://')
# #sqlalchemy_url = Literal("mysql://gjh:50uthf0rk@localhost:3306/test")

# class SQLAlchemyBaseGraphTestCase(test_graph.GraphTestCase):
#     storetest = True
#     storename = "SQLAlchemyBase"
#     uri = sqlalchemy_url
#     def setUp(self):
#         test_graph.GraphTestCase.setUp(self, uri=self.uri, storename=self.storename)
    
#     def tearDown(self):
#         self.create = False
#         test_graph.GraphTestCase.tearDown(self, uri=self.uri)
   
#     def testStatementNode(self):
#         raise SkipTest("RDF Statements not supported in FOPL model.")

# # class SQLAlchemyBaseContextTestCase(test_context.ContextTestCase):
# #     storetest = True
# #     storename = "SQLAlchemyBase"
# #     uri = sqlalchemy_url
# #     def setUp(self):
# #         test_context.ContextTestCase.setUp(self, uri=self.uri, storename=self.storename)

# #     def tearDown(self):
# #         self.create = False
# #         test_context.ContextTestCase.tearDown(self, uri=self.uri)
   
# SQLAlchemyBaseGraphTestCase.storetest = True
# # SQLAlchemyBaseContextTestCase.storetest = True
