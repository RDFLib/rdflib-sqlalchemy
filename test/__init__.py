from rdflib import plugin
from rdflib import store

plugin.register('SQLAlchemy', store.Store,
        'rdflib_sqlalchemy.SQLAlchemy', 'SQLAlchemy')

plugin.register('SQLAlchemyASS', store.Store,
        'rdflib_sqlalchemy.SQLAlchemyASS', 'SQLAlchemy')

plugin.register('SQLAlchemyBase', store.Store,
        'rdflib_sqlalchemy.SQLAlchemyBase', 'SQLAlchemy')

plugin.register('SQLAlchemyFOPL', store.Store,
        'rdflib_sqlalchemy.SQLAlchemyFOPL', 'SQLAlchemy')
