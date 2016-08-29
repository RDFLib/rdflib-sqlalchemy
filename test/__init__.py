from rdflib import plugin
from rdflib import store

plugin.register(
    "SQLAlchemy",
    store.Store,
    "rdflib_sqlalchemy.store",
    "SQLAlchemy",
)
