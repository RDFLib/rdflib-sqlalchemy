#!/usr/bin/env python
"""Setup."""
# -*- coding: utf-8 -*-
import sys
import re


def setup_python3():
    """Taken from "distribute" setup.py."""
    from distutils.filelist import FileList
    from distutils import dir_util, file_util, util
    from os.path import join, exists

    tmp_src = join("build", "src")
    # Not covered by "setup.py clean --all", so explicit deletion required.
    if exists(tmp_src):
        dir_util.remove_tree(tmp_src)
    # log.set_verbosity(1)
    fl = FileList()
    for line in open("MANIFEST.in"):
        if not line.strip():
            continue
        fl.process_template_line(line)
    dir_util.create_tree(tmp_src, fl.files)
    outfiles_2to3 = []
    for f in fl.files:
        outf, copied = file_util.copy_file(f, join(tmp_src, f), update=1)
        if copied and outf.endswith(".py"):
            outfiles_2to3.append(outf)

    util.run_2to3(outfiles_2to3)

    # arrange setup to use the copy
    sys.path.insert(0, tmp_src)

    return tmp_src

kwargs = dict(
    install_requires=[
        "rdflib>=4.0",
        "six>=1.10.0",
        "SQLAlchemy",
    ],
    tests_require="coveralls",
)

if sys.version_info[0] >= 3:
    from setuptools import setup
    # kwargs['use_2to3'] = True  # is done in setup_python3 above already
    kwargs['requires'] = []
    kwargs['src_root'] = setup_python3()
    assert setup
else:
    try:
        from setuptools import setup
        assert setup
        kwargs['test_suite'] = "nose.collector"

    except ImportError:
        from distutils.core import setup


def find_version(filename):
    """Find version.

    We have to do this because we can't import it in Python 3 until
    its been automatically converted in the setup process.
    """
    _version_re = re.compile(r'__version__ = "(.*)"')
    for line in open(filename):
        version_match = _version_re.match(line)
        if version_match:
            return version_match.group(1)

__version__ = find_version('rdflib_sqlalchemy/__init__.py')

setup(
    name='rdflib-sqlalchemy-redux',
    version=__version__,
    description="rdflib extension adding SQLAlchemy as an AbstractSQLStore back-end store",
    author="Graham Higgins",
    author_email="gjhiggins@gmail.com",
    url="http://github.com/globality-corp/rdflib-sqlalchemy",
    packages=["rdflib_sqlalchemy"],
    download_url="https://github.com/RDFLib/rdflib-sqlalchemy/zipball/master",
    license="BSD",
    platforms=["any"],
    long_description="""
    SQLAlchemy store formula-aware implementation.  It stores its triples in
    the following partitions:

    * Asserted non rdf:type statements
    * Asserted rdf:type statements (in a table which models Class membership).
      The motivation for this partition is primarily improved query speed and
      scalability as most graphs will always have more rdf:type statements than
      others.
    * All Quoted statements

    In addition it persists namespace mappings in a separate table
    """,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: BSD License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: OS Independent",
        "Natural Language :: English",
    ],
    entry_points={
        'rdf.plugins.store': [
            # 'SQLAlchemy = rdflib_sqlalchemy.SQLAlchemy:SQLAlchemy',
            # 'SQLAlchemyBase = rdflib_sqlalchemy.SQLAlchemyBase:SQLAlchemy',
        ],
    },
    **kwargs
)
