from rdflib import plugin
from rdflib import store, query

import sys  # sop to Hudson
sys.path.insert(0, '/var/lib/tomcat6/webapps/hudson/jobs/rdfextras')

plugin.register('SQLAlchemy', store.Store,
        'rdflib_sqlalchemy.SQLAlchemy', 'SQLAlchemy')

plugin.register('SQLAlchemyASS', store.Store,
        'rdflib_sqlalchemy.SQLAlchemyASS', 'SQLAlchemy')

plugin.register('SQLAlchemyBase', store.Store,
        'rdflib_sqlalchemy.SQLAlchemyBase', 'SQLAlchemy')

plugin.register('SQLAlchemyFOPL', store.Store,
        'rdflib_sqlalchemy.SQLAlchemyFOPL', 'SQLAlchemy')

# A sop to Hudson (I thought this was no longer necessary)
plugin.register('sparql', query.Processor,
                    'rdfextras.sparql.processor', 'Processor')
plugin.register('sparql', query.Result,
                    'rdfextras.sparql.query', 'SPARQLQueryResult')
