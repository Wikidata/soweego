Development and production environments
=======================================

Get started
-----------

1. `Install Docker <https://www.docker.com/get-started>`__;
2. In your command line launch the following commands:

.. code:: bash

   git clone https://github.com/Wikidata/soweego.git
   cd soweego


.. _dev:

Development environment
-----------------------

It's useful when you are developing or testing some features. In this
environment, you don't need to be afraid of breaking stuff.

It provides you with a MariaDB instance and a BASH shell ready to run
*soweego* CLI commands. You are free to change *soweego* code while the
shell is running, the code is synced.

What do you need to run it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  `Docker <https://www.docker.com/get-started>`__
-  `Docker Compose <https://docs.docker.com/compose/install/>`__

How do you run it?
~~~~~~~~~~~~~~~~~~

1. In your terminal, move to the project root;
2. Launch ``./scripts/docker/launch_test.sh``;
3. You are now in a Docker container BASH shell with a fully working
   soweego instance;
4. You are set. To check if it's working, try ``python -m soweego``.

.. _launch_testsh-options:

launch_test.sh options
^^^^^^^^^^^^^^^^^^^^^^

========== ================== ======================= ================================================================================
**Option** **Expected Value** **Default Value**       **Description**
========== ================== ======================= ================================================================================
-s         directory path     ``/tmp/soweego_shared`` Tells docker which folder in your machine will be shared with soweego container.
========== ================== ======================= ================================================================================

How do you connect with the local database instance?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The test environment comes with a running
`MariaDB <https://mariadb.com/>`__ instance. To query it from your
terminal:

1. ``docker exec -it soweego_db_1 /bin/bash``;
2. ``mysql -uroot -hlocalhost -pdba soweego``.

Production environment
----------------------

It's useful when you need to run *soweego* against the Wikimedia
database. It is also helpful to run the system against a custom
database. Creating a credentials file is all you need to do to chose the
database. Note: you need access to the Wikimedia infrastructure to run
*soweego* on it.

This environment provides you with a BASH shell ready to run *soweego*
CLI commands. You are free to change *soweego* code while the shell is
running, the code is synced.

.. _what-do-you-need-to-run-it-1:

What do you need to run it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  `Docker <https://www.docker.com/get-started>`__
-  Database credentials file, like
   ``${PROJECT_ROOT}/soweego/importer/resources/db_credentials.json``.

.. _how-do-you-run-it-1:

How do you run it?
~~~~~~~~~~~~~~~~~~

1. In your terminal, move to the project root;
2. Launch ``./scripts/docker/launch_prod.sh``;
3. You are now in a Docker container BASH shell with a fully working
   *soweego* instance;
4. You are set. To check if it's working, try ``python -m soweego``.

.. _launch_prodsh-options:

launch_prod.sh options
^^^^^^^^^^^^^^^^^^^^^^

========== ================== ================================================================== ==================================================================================
**Option** **Expected Value** **Default Value**                                                  **Description**
========== ================== ================================================================== ==================================================================================
-s         directory path     ``/tmp/soweego_shared``                                            Tells docker which folder in your machine will be shared with *soweego* container.
-c         file path          ``${PROJECT_ROOT}/soweego/importer/resources/db_credentials.json`` Sets which file in your machine *soweego* will read for database credentials.
========== ================== ================================================================== ==================================================================================
