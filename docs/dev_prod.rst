Development and production environments
=======================================

.. note::

   as a *soweego* user or contributor, you will typically use the :ref:`dev`.
   The :ref:`prod` is tailored for
   `Cloud VPS project <https://tools.wmflabs.org/openstack-browser/project/soweego>`_ members.


First of all
------------

Install `Docker <https://docs.docker.com/install/>`_, then clone soweego::

   $ git clone https://github.com/Wikidata/soweego.git
   $ cd soweego


.. _dev:

Development environment
-----------------------

Use it to run or play around *soweego* on your local machine.
And to contribute new features, of course!

This environment ships with a `MariaDB <https://mariadb.com/>`_ database instance
and a `BASH <https://www.gnu.org/software/bash/>`_ shell.
It is ready to run :ref:`clidoc`.
Feel free to hack *soweego* while the environment is running: your code is synced.

Get set
~~~~~~~

Just install `Docker Compose <https://docs.docker.com/compose/install/>`_.

Go
~~

::

   $ ./docker/run.sh
   Building soweego
   ...

   root@70c9b4894a30:/app/soweego#

You are now in a BASH shell with a fully working *soweego* instance.
Check if everything went fine with a shot of ::

   python -m soweego


``run.sh`` options
~~~~~~~~~~~~~~~~~~

========== ================== ======================= =================================================================================
**Option** **Expected value** **Default value**       **Description**
========== ================== ======================= =================================================================================
``-s``     a directory path   ``/tmp/soweego_shared`` Directory shared between the *soweego* Docker container and your local filesystem
========== ================== ======================= =================================================================================

Access the local database instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As easy as::

   $ docker exec -it soweego_db_1 /bin/bash
   root@0f51e7c512df:/# mysql -uroot -hlocalhost -pdba soweego
   MariaDB [soweego]>


.. _prod:

Production environment
----------------------

*soweego* lives in a Wikimedia
`Cloud VPS project <https://tools.wmflabs.org/openstack-browser/project/soweego>`_,
and this is the environment deployed there.
Please contact the project administrators if you want to help with the VPS machine.

You can also use it to run *soweego* on a machine that already has a working database
(typically a server).

This environment ships with a `BASH <https://www.gnu.org/software/bash/>`_ shell
ready to run :ref:`clidoc`.
Feel free to hack *soweego* while the environment is running: your code is synced.


Get set
~~~~~~~

Just create a credentials JSON file like this::

   {
       "DB_ENGINE": "mysql+pymysql",
       "HOST": "${IP_ADDRESS}",
       "USER": "${DB_USER}",
       "PASSWORD": "${DB_PASSWORD}",
       "TEST_DB": "soweego",
       "PROD_DB": "${DB_NAME}"
   }

Don't forget to set the ``${...}`` variables!


Go
~~

::

   $ ./docker/prod.sh -c ${YOUR_CREDENTIALS_FILE}
   Sending build context to Docker daemon
   ...

   root@62c602c23fa9:/app/soweego#

You are now in a BASH shell with a fully working *soweego* instance.
Check if everything went fine with a shot of ::

   python -m soweego


``prod.sh`` options
^^^^^^^^^^^^^^^^^^^

========== ================== =============================================================== =================================================================================
**Option** **Expected value** **Default value**                                                  **Description**
========== ================== =============================================================== =================================================================================
``-s``     a directory path   ``/tmp/soweego_shared``                                         Directory shared between the *soweego* Docker container and your local filesystem
``-c``     a file path        ``${PROJECT_ROOT}/soweego/importer/resources/credentials.json`` Credentials file
========== ================== =============================================================== =================================================================================
