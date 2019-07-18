.. soweego documentation master file, created by
   sphinx-quickstart on Mon Jun  3 13:12:22 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

soweego: link Wikidata to large catalogs
========================================

.. image:: https://travis-ci.com/Wikidata/soweego.svg?branch=master
   :target: https://travis-ci.com/Wikidata/soweego
   :alt: Build Status

.. image:: https://readthedocs.org/projects/soweego/badge/?version=latest 
   :target: https://soweego.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://img.shields.io/github/license/Wikidata/soweego.svg
   :target: https://www.gnu.org/licenses/gpl-3.0.html
   :alt: License


*soweego* is a pipeline that connects `Wikidata <https://wikidata.org/>`_ to large-scale third-party catalogs.

*soweego* is the only system that makes *statisticians, epidemiologists, historians,* and *computer scientists* agree.
Why? Because it performs *record linkage, data matching,* and *entity resolution* at the same time.
Too easy, they all seem to be `synonyms <https://en.wikipedia.org/wiki/Record_linkage#Naming_conventions>`_!

Oh, *soweego* also embeds `Machine Learning <https://en.wikipedia.org/wiki/Machine_learning>`_ and advocates for `Linked Data <https://en.wikipedia.org/wiki/Linked_data>`_.


Official Project Page
---------------------

*soweego* is made possible thanks to the `Wikimedia Foundation <https://wikimediafoundation.org/>`_:

https://meta.wikimedia.org/wiki/Grants:Project/Hjfocs/soweego


Highlights
----------

- Run the whole :ref:`pipeline <run-the-pipeline>`, or
- use the :ref:`command line <use-the-command-line>`;
- :mod:`import <soweego.importer>` large catalogs into a SQL database;
- :mod:`gather <soweego.wikidata>` live Wikidata datasets;
- :mod:`connect <soweego.linker>` them to target catalogs via *rule-based* and *supervised* linkers;
- :mod:`upload <soweego.ingester>` links to Wikidata and `Mix'n'match <https://tools.wmflabs.org/mix-n-match/>`_;
- :mod:`synchronize <soweego.validator.checks>` Wikidata to imported catalogs;
- :mod:`enrich <soweego.validator.enrichment>` Wikidata items with relevant statements.


Get Ready
---------

Install `Docker <https://docs.docker.com/install/>`_
and `Compose <https://docs.docker.com/compose/install/>`_,
then enter *soweego*::

   $ git clone https://github.com/Wikidata/soweego.git
   $ cd soweego
   $ ./docker/run.sh
   Building soweego
   ...

   root@70c9b4894a30:/app/soweego#

Now it's too late to get out!


.. _run-the-pipeline:

Run the Pipeline
----------------

Piece of cake:

.. code-block:: text

   :/app/soweego# python -m soweego run CATALOG

Pick ``CATALOG`` from ``discogs``, ``imdb``, or ``musicbrainz``.

These steps are executed by default:

1. import the target catalog into a local database;
2. link Wikidata to the target with a supervised linker;
3. synchronize Wikidata to the target.

Results are in ``/app/shared/results``.


.. _use-the-command-line:

Use the Command Line
--------------------

You can launch every single *soweego* action with CLI commands:

.. code-block:: text

   :/app/soweego# python -m soweego
   Usage: soweego [OPTIONS] COMMAND [ARGS]...

     Link Wikidata to large catalogs.

   Options:
     -l, --log-level <TEXT CHOICE>...
                              Module name followed by one of [DEBUG, INFO,
                              WARNING, ERROR, CRITICAL]. Multiple pairs
                              allowed.
     --help                   Show this message and exit.

   Commands:
     importer  Import target catalog dumps into a SQL database.
     ingester  Take soweego output into Wikidata items.
     linker    Link Wikidata items to target catalog identifiers.
     run       Launch the whole pipeline.
     sync      Sync Wikidata to target catalogs.

Just two things to remember:

1. you can always get ``--help``;
2. each command may have sub-commands.

Find all details in the :ref:`cli_docs`.


How-tos
-------

.. toctree::
   :maxdepth: 1

   pipeline
   new_catalog
   dev_prod


.. _cli_docs:

CLI Documentation
-----------------

.. toctree::
   :maxdepth: 2

   cli


API Documentation
-----------------

.. toctree::
   :maxdepth: 2

   importer
   models
   ingester
   linker
   validator
   wikidata


Contribute
----------

.. note:: the best way is to :ref:`new`.

Please also have a look here:

.. toctree::
   :maxdepth: 2

   contribute


Experiments & notes
-------------------

.. toctree::
   :maxdepth: 1
   
   experiments
   evaluations
   recordlinkage


License
-------

The source code is under the terms of the `GNU General Public License, version 3 <https://www.gnu.org/licenses/gpl.html>`_.
