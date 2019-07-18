.. _clidoc:

The command line
================

.. note:: start your exploration journey of the command line interface (*CLI*) with ::

   $ python -m soweego

As a reminder, make sure you are inside Docker::

   $ cd soweego && ./docker/run.sh
   Building soweego

   ...

   root@70c9b4894a30:/app/soweego#


.. _importer:

Importer
--------

.. code-block:: text

   python -m soweego importer
   Usage: soweego importer [OPTIONS] COMMAND [ARGS]...

     Import target catalog dumps into a SQL database.

   Options:
     --help  Show this message and exit.

   Commands:
     check_urls  Check for rotten URLs of an imported catalog.
     import      Download, extract, and import a supported catalog.

.. click:: soweego.importer.importer:check_links_cli
   :prog: check_urls

.. click:: soweego.importer.importer:import_cli
   :prog: import


Ingester
--------

.. code-block:: text

   python -m soweego ingester
   Usage: soweego ingester [OPTIONS] COMMAND [ARGS]...

     Take soweego output into Wikidata items.

   Options:
     --help  Show this message and exit.

   Commands:
     delete       Delete invalid identifiers.
     deprecate    Deprecate invalid identifiers.
     identifiers  Add identifiers.
     mnm          Upload matches to the Mix'n'match tool.
     people       Add statements to Wikidata people.
     works        Add statements to Wikidata works.

.. click:: soweego.ingester.wikidata_bot:delete_cli
   :prog: delete

.. click:: soweego.ingester.wikidata_bot:deprecate_cli
   :prog: deprecate

.. click:: soweego.ingester.wikidata_bot:identifiers_cli
   :prog: identifiers

.. click:: soweego.ingester.mix_n_match_client:cli
   :prog: mnm

.. click:: soweego.ingester.wikidata_bot:people_cli
   :prog: people

.. click:: soweego.ingester.wikidata_bot:works_cli
   :prog: works


.. _linker:

Linker
------

.. code-block:: text

   python -m soweego linker
   Usage: soweego linker [OPTIONS] COMMAND [ARGS]...

     Link Wikidata items to target catalog identifiers.

   Options:
     --help  Show this message and exit.

   Commands:
     baseline  Run a rule-based linker.
     evaluate  Evaluate the performance of a supervised linker.
     extract   Extract Wikidata links from a target catalog dump.
     link      Run a supervised linker.
     train     Train a supervised linker.

.. click:: soweego.linker.baseline:cli
   :prog: baseline

.. click:: soweego.linker.evaluate:cli
   :prog: evaluate

.. click:: soweego.linker.baseline:extract_cli
   :prog: extract

.. click:: soweego.linker.link:cli
   :prog: link

.. click:: soweego.linker.train:cli
   :prog: train


.. _pipeline:

Pipeline
--------

.. code-block:: text

   python -m soweego run

.. click:: soweego.pipeline:cli
   :prog: run


.. _validator:

Validator AKA Sync
------------------

.. code-block:: text

   python -m soweego sync
   Usage: soweego sync [OPTIONS] COMMAND [ARGS]...

     Sync Wikidata to target catalogs.

   Options:
     --help  Show this message and exit.

   Commands:
     bio    Validate identifiers against biographical data.
     ids    Check if identifiers are still alive.
     links  Validate identifiers against links.
     works  Generate statements about works by people.

.. click:: soweego.validator.checks:bio_cli
   :prog: bio

.. click:: soweego.validator.checks:dead_ids_cli
   :prog: ids

.. click:: soweego.validator.checks:links_cli
   :prog: links

.. click:: soweego.validator.enrichment:works_people_cli
   :prog: works
