#!/bin/bash -ex

if [ "$DB" = 'pgsql' ]; then
    psql -c 'DROP DATABASE IF EXISTS test;' -U postgres
    psql -c 'create database test;' -U postgres
    export DBURI='postgresql+psycopg2://postgres@localhost/test'
fi

if [ "$DB" = 'mysql' ]; then
    mysql -e 'CREATE SCHEMA test DEFAULT CHARACTER SET utf8;'
    if [[ "$TRAVIS_PYTHON_VERSION" =~ '.*3\.[4567].*' ]] ; then
        export DBURI='mysql+mysqlconnector://test@localhost/test?charset=utf8'
    else
        export DBURI='mysql+mysqldb://test@localhost/test?charset=utf8'
    fi
fi

if [ "$DB" = 'sqlite' ]; then
    export DBURI='sqlite:///%(here)s/test.sqlite'
fi
