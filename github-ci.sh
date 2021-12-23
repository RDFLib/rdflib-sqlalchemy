#!/bin/bash -ex

if [ "$DB" = 'pgsql' ]; then
    echo localhost:5432:postgres:postgres:postgres > ~/.pgpass
    chmod 0600 ~/.pgpass
    psql -w -h localhost -c 'DROP DATABASE IF EXISTS test;' -U postgres
    psql -w -h localhost -c 'create database test;' -U postgres
    # ~/.pgpass doesn't work apparently...whatever
    export DBURI='postgresql+psycopg2://postgres:postgres@localhost/test'
fi

if [ "$DB" = 'mysql' ]; then
    export DBURI='mysql+mysqldb://test:mysql@127.0.0.1/test?charset=utf8'
fi

if [ "$DB" = 'sqlite' ]; then
    export DBURI='sqlite:///test.sqlite'
fi
