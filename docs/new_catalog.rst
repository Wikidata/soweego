Import a new catalog
====================

1. Ensure you have the `test
   environment <https://github.com/Wikidata/soweego/wiki/Test-and-production-environments#test-environment>`__
   up and running;
2. create a model file for the database you want to import in
   ``${PROJECT_ROOT}/soweego/importer/models/``;
3. call it ``${NEW_DATABASE}_entity.py`` and paste the snippet below. It
   is enough to replace ``${NEW_DATABASE}`` with your database name.
   Other variables (marked with a leading ``$``) are optional;
4. **optional:** you can define database-specific columns, see ``TODO``.
   Column names **must be unique**: no overlapping among classes.

.. code:: py

   #!/usr/bin/env python3
   # -*- coding: utf-8 -*-

   """${NEW_DATABASE} SQL Alchemy ORM model"""

   __author__ = '${YOUR_NAME_HERE}'
   __email__ = '${YOUR_EMAIL_HERE}'
   __version__ = '1.0'
   __license__ = 'GPL-3.0'
   __copyright__ = 'Copyleft ${YEAR}, ${YOUR_NAME_HERE}'

   from sqlalchemy import Column, ForeignKey, String
   from sqlalchemy.ext.declarative import declarative_base

   from soweego.importer.models.base_entity import BaseEntity
   from soweego.importer.models.base_link_entity import BaseLinkEntity

   BASE = declarative_base()

   class ${NEW_DATABASE}Entity(BaseEntity, BASE):
       __tablename__ = '${NEW_DATABASE}'
       __mapper_args__ = {
           'polymorphic_identity': __tablename__,
           'concrete': True}
       # TODO Optional: define database-specific columns here
       # For instance:
       # birth_place = Column(String(255))

   class ${NEW_DATABASE}LinkEntity(BaseLinkEntity, BASE):
       __tablename__ = '${NEW_DATABASE}_link'
       __mapper_args__ = {
           'polymorphic_identity': __tablename__,
           'concrete': True}
       catalog_id = Column(String(32), ForeignKey(${NEW_DATABASE}Entity.catalog_id), 
                           index=True)

1. create the file
   ``${PROJECT_ROOT}/soweego/importer/${NEW_DATABASE}_dump_downloader.py``;
2. define a class ``${NEW_DATABASE}DumpDownloader(BaseDumpDownloader)``;
3. override ``BaseDumpDownloader`` methods:

   -  ``import_from_dump`` creates ``${NEW_DATABASE}Entity`` and
      ``${NEW_DATABASE}LinkEntity`` instances for each entity and stores
      it in the database. See the instructions below;
   -  ``dump_download_url`` computes and returns the latest dump URL.
      The override is optional: if you don't implement it, you'll always
      have to call the import of your database with the
      ``--download-url`` option (see later).

Instructions to store entities in database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Setup:

::

   db_manager = DBManager()
   db_manager.drop(${NEW_DATABASE}Entity)
   db_manager.create(${NEW_DATABASE}Entity)

Creating a transaction:

::

   session = db_manager.new_session()

Adding an entity to a transaction

::

   current_entity = ${NEW_DATABASE}Entity()
   ...
   session.add(current_entity)

Committing a transaction:

::

   session.commit()

Keep your sessions as small as possibile!

Set up the CLI to import your database
--------------------------------------

``${PROJECT_ROOT}/soweego/importer/importer.py`` contains the following
CLI command:

.. code:: py

   @click.command()
   @click.argument('catalog', type=click.Choice(['discogs', 'musicbrainz']))
   @click.option('--download-url', '-du', default=None)
   @click.option('--output', '-o', default='output', type=click.Path())
   def import_cli(catalog: str, download_url: str, output: str) -> None:
       """Check if there is an updated dump in the output path;
          if not, download the dump"""
       importer = Importer()
       downloader = BaseDumpDownloader()

       if catalog == 'discogs':
           downloader = DiscogsDumpDownloader()
       elif catalog == 'musicbrainz':
           downloader = MusicBrainzDumpDownloader()

       importer.refresh_dump(
           output, download_url, downloader)

Add an ``elif`` case for your database and make sure you set the
appropriate ``downloader`` for your database.

The same database name you choose for the if statement needs to be added
in the list:
``@click.argument('catalog', type=click.Choice(['discogs', 'musicbrainz']))``.

Running the import process
--------------------------

1. Ensure to be in `test or production
   mode <https://github.com/Wikidata/soweego/wiki/How-do-I-test-soweego-on-my-machine%3F>`__.

2. run
   ``python -m soweego importer import_catalog ${YOUR_DATABASE_NAME}``

   You have the following options:

   -  ``--output``, ``-o``, for setting the output folder in which the
      dump will be stored
   -  ``--download-url``, ``-du``, for specifying a dump URL to download
