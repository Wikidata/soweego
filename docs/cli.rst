The Command Line
================

Importer
--------

Usage::

   $ python -m soweego importer ...

.. click:: soweego.importer.importer:import_cli
   :prog: import

.. click:: soweego.importer.importer:check_links_cli
   :prog: check_urls


Ingestor
--------

Usage::

   $ python -m soweego ingest ...

.. click:: soweego.ingestor.wikidata_bot:delete_cli
   :prog: deletion

.. click:: soweego.ingestor.wikidata_bot:deprecate_cli
   :prog: deprecation

.. click:: soweego.ingestor.wikidata_bot:identifiers_cli
   :prog: identifiers

.. click:: soweego.ingestor.mix_n_match_client:cli
   :prog: mnm

.. click:: soweego.ingestor.wikidata_bot:people_cli
   :prog: people

.. click:: soweego.ingestor.wikidata_bot:works_cli
   :prog: works


Linker
------

Usage::

   $ python -m soweego linker ...

.. click:: soweego.linker.baseline:cli
   :prog: baseline

.. click:: soweego.linker.evaluate:cli
   :prog: evaluate

.. click:: soweego.linker.baseline:extract_cli
   :prog: extract

.. click:: soweego.linker.train:cli
   :prog: train

.. click:: soweego.linker.link:cli
   :prog: link


Pipeline
--------

Usage::

   $ python -m soweego ...

.. click:: soweego.pipeline:cli
   :prog: run


Validator AKA Sync
------------------

Usage::

   $ python -m soweego sync ...
   
.. click:: soweego.validator.checks:dead_ids_cli
   :prog: ids

.. click:: soweego.validator.checks:links_cli
   :prog: links

.. click:: soweego.validator.checks:bio_cli
   :prog: bio

.. click:: soweego.validator.enrichment:works_people_cli
   :prog: works
