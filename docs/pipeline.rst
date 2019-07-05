Run soweego
===========

*soweego* can be seen as a **pipeline of submodules**. Maybe, is best to
say that we designed it explicitly in this way. They can be combined at
will, but in the following lines, you will read how we do it.

Our flow starts by running the **importing process**, which translates
the target dumps in structured database tables. After that, we run the
**linking process**. In this step, the linker itself gathers the right
up to date dataset from Wikidata and tries to match it with the
previously imported data. The last step we execute is **validation**.
Basically, it scans the linked entities available in Wikidata to perform
some naive quality checks. Each entity approved by the validator is
consequently enriched with all the assertions minable from our imported
data.

What do I need to run your setup?
---------------------------------

First of all, you need Docker up and running on the system. Then, since
our “production” environment has benefited from Wikimedia Foundations’s
database, you need to provide soweego a working database yourself 
(MariaDB 10.36 is the only database tested). To tell soweego where
to find your database, you need to create a JSON file with the following
structure:

.. code:: json

   {
       "DB_ENGINE": "mysql+pymysql",
       "HOST": "*ip address or equivalent*",
       "USER": "*database user*",
       "PASSWORD": "*database user password*",
       "TEST_DB": "soweego",
       "PROD_DB": "*database name*",
       "WIKIDATA_API_PASSWORD": "",    # Optional wikidata api login
       "WIKIDATA_API_USER": ""         # Optional wikidata api login
   }

Finally, ensure to have a folder in which soweego will write
results/helper files. soweego favourite food is disk space, but usually
with 20GB it should be sated.

How do I actually run it?
-------------------------

1. Clone soweego from `GitHub <https://github.com/wikidata/soweego/>`__;
2. Open a bash and move in the project root;
3. Choose a target to run eg Musicbrainz, Discogs, ImDB.
4. Launch the following command replacing the variables with your
   absolute paths and append your target:
   ``./docker/launch_pipeline.sh -c ${ABSOLUTE_PATH_TO_THE_JSON} -s ${ABSOLUTE_ PATH_TO_THE_OUTPUT_FOLDER} imdb``

Additional parameters tailable to your command:

+-----------------------------------+-----------------------------------+
| Argument                          | Description                       |
+===================================+===================================+
| ``--validator`` /                 | Enables/disables the validation   |
| ``--no-validator``                | step                              |
+-----------------------------------+-----------------------------------+
| ``--importer`` /                  | Enables/disables the importing    |
| ``--no-importer``                 | step                              |
+-----------------------------------+-----------------------------------+
| ``--linker`` / ``--no-linker``    | Enables/disables the linking step |
+-----------------------------------+-----------------------------------+
| ``--upload`` / ``--no-upload``    | Enables/disable the upload to     |
|                                   | Wikidata of the results           |
+-----------------------------------+-----------------------------------+

Important
~~~~~~~~~

The command does not only run soweego, but it takes care of some side
tasks. Initially, it backups the folder you give as the parameter. It will keep atmost 3 backups.
When creating the 4th backup, the oldest
is deleted. After the archiving step, the given folder is emptied.
Subsequently, it checks out the master branch and pulls the latest
changes (deleting all the pending edits in the local repository).
Finally, our soweego setup is launched.

Under the hood
--------------

The submodules arrangment is actually defined in ``pipeline.py`` and is
launched by ``python -m soweego run``. Our setup script launches
``python -m soweego run`` as latest command and appends all the
arguments from the target (eg. musicbrainz, discogs, imdb) onwards.

Cron jobs
---------

Our setup is launched periodically for each target. We obviously rely on
cron jobs to achieve this. You can find in ``scripts/cron`` the helpers
we call though the cron tab. They can be reused easily just by changing
all the paths to adhere to your environment setup.
