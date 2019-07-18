Run the pipeline
================

*soweego* is a pipeline of **Python modules** by design.
Each module can be used alone or combined with others at will.

In this page, you will grasp the typical workflow:

1. :ref:`import <importer>` the dumps of a given target catalog into a SQL database
2. :ref:`link <linker>` the imported catalog to Wikidata
3. :ref:`sync <validator>` Wikidata to the imported catalog


Get set
-------

1. Install `Docker <https://docs.docker.com/install/>`_
2. install `MariaDB <https://mariadb.com/downloads/#mariadb_platform>`_
3. create a credentials JSON file like this:

::

   {
       "DB_ENGINE": "mysql+pymysql",
       "HOST": "${DB_IP_ADDRESS}",
       "USER": "${DB_USER}",
       "PASSWORD": "${DB_PASSWORD}",
       "TEST_DB": "soweego",
       "PROD_DB": "${DB_NAME}",
       "WIKIDATA_API_USER": "${WIKI_USER_NAME}",
       "WIKIDATA_API_PASSWORD": "${WIKI_PASSWORD}"
   }

``WIKIDATA_API_USER`` and ``WIKIDATA_API_PASSWORD`` are optional:
set them to run authenticated requests against the
`Wikidata Web API <https://www.wikidata.org/w/api.php>`_.
If you have a `Wikidata bot account <https://www.wikidata.org/wiki/Wikidata:Bots>`_,
processing will speed up.

*soweego*'s favourite food is disk space, so make sure you have enough:
**20 GB** should sate its appetite.


Go
--

::

   $ git clone https://github.com/Wikidata/soweego.git
   $ cd soweego
   $ ./docker/pipeline.sh -c ${CREDENTIALS_FILE} -s ${OUTPUT_FOLDER} ${CATALOG}

``${OUTPUT_FOLDER}`` is a path to a folder on your local filesystem:
this is where all *soweego* output goes.
Pick ``${CATALOG}`` from ``discogs``, ``imdb``, or ``musicbrainz``.


``pipeline.sh``
~~~~~~~~~~~~~~~

This script does not only run *soweego*, but also takes care of some side tasks:

- backs up the output folder in a tar ball
- keeps at most 3 backups
- empties the output folder
- pulls the latest *soweego* master branch.
  **N.B.:** this will **erase any pending edits**
  in the local git repository

==================================== =========== ==============================
             **Flag**                **Default**        **Description**
==================================== =========== ==============================
``--importer`` / ``--no-importer``   enabled     enable / disable the importer
``--linker`` / ``--no-linker``       enabled     enable / disable the linker
``--validator`` / ``--no-validator`` enabled     enable / disable the validator
``--upload`` / ``--no-upload``       disabled    enable / disable the upload
                                                 of results to Wikidata
==================================== =========== ==============================


Under the hood
--------------

The actual pipeline is implemented in ``soweego/pipeline.py``,
so you can also launch it with ::
   
   python -m soweego run
   
See :ref:`clidoc` and :ref:`pipeline` for more details.


Cron jobs
---------

*soweego* periodically runs pipelines for each supported catalog via
`cron <https://en.wikipedia.org/wiki/Cron>`_ jobs.
You can find ``crontab``-ready scripts in the ``scripts/cron`` folder.
Feel free to reuse them! Just remember to set the appropriate paths.
