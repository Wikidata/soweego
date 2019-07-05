Import a new catalog
====================

1. Ensure you have the `test
   environment <https://github.com/Wikidata/soweego/wiki/Test-and-production-environments#test-environment>`__
   up and running;
2. create a model file for the database you want to import in
   ``${PROJECT_ROOT}/soweego/importer/models/``;
3. call it ``${NEW_DATABASE}_entity.py`` and paste the snippet below. It is enough to replace ``${NEW_DATABASE}`` with your database name and ``${NEW_ENTITY_NAME}`` with a word describing what this entity is about e.g musician, painter.
   Other variables (marked with a leading ``$``) are optional;
4. **optional:** you can define database-specific columns, see ``TODO``.
   Column names **must be unique**: no overlapping among the class you define and the BaseEntity class.

.. code:: py

   #!/usr/bin/env python3
   # -*- coding: utf-8 -*-

   """${NEW_DATABASE} SQL Alchemy ORM model"""

   __author__ = '${YOUR_NAME_HERE}'
   __email__ = '${YOUR_EMAIL_HERE}'
   __version__ = '1.0'
   __license__ = 'GPL-3.0'
   __copyright__ = 'Copyleft ${YEAR}, ${YOUR_NAME_HERE}'

   from sqlalchemy import Column, String
   from sqlalchemy.ext.declarative import declarative_base

   from soweego.importer.models.base_entity import BaseEntity, BaseRelationship
   from soweego.importer.models.base_link_entity import BaseLinkEntity

   BASE = declarative_base()

   class ${NEW_DATABASE}${NEW_ENTITY_NAME}Entity(BaseEntity):
       """Describes a ${NEW_ENTITY_NAME} of ${NEW_DATABASE}"""

       __tablename__ = '${NEW_DATABASE}_${NEW_ENTITY_NAME}'
       __mapper_args__ = {'polymorphic_identity': __tablename__, 'concrete': True}

       # TODO Optional: define database-specific columns here
       # For instance:
       # birth_place = Column(String(255))

5. create the file
   ``${PROJECT_ROOT}/soweego/importer/${NEW_DATABASE}_dump_extractor.py``;
6. define a class ``${NEW_DATABASE}DumpExtractor(BaseDumpExtractor)``;
7. override ``BaseDumpExtractor`` methods:

   -  ``extract_and_populate`` is in charge to create an instance of ``${NEW_DATABASE}${NEW_ENTITY_NAME}Entity`` for each entity in the dump and to store
      it in the database. See the instructions below;
   -  ``get_dump_download_urls`` computes the latest list of URLs forming the dump. Tipically, there will be only an URL, but in some case the dumps are given in multiple archives.
   
8. If you still have doubts, try to check out Musicbrainz, Discogs or Imdb extractors.

Instructions to store entities in database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Setup:

::

   db_manager = DBManager()
   db_manager.drop(${NEW_DATABASE}${NEW_ENTITY_NAME})
   db_manager.create(${NEW_DATABASE}${NEW_ENTITY_NAME})

Creating a transaction:

::

   session = db_manager.new_session()

Adding an entity to a transaction

::

   current_entity = ${NEW_DATABASE}${NEW_ENTITY_NAME}Entity()
   ...
   # Fill the fields of current_entity
   ...
   session.add(current_entity)

Committing a transaction:

::

   session.commit()

Keep your sessions as small as possibile!

Set up the CLI to import your database
--------------------------------------

Add a couple of keys for your database in ``${PROJECT_ROOT}/soweego/commons/keys.cs``:

.. code:: py

   # Supported catalogs
   DISCOGS = 'discogs'
   IMDB = 'imdb'
   MUSICBRAINZ = 'musicbrainz'
   TWITTER = 'twitter'
   ${NEW_DATABASE} = '${NEW_DATABASE}'
   
   # Supported entities
   # People
   ACTOR = 'actor'
   BAND = 'band'
   DIRECTOR = 'director'
   PRODUCER = 'producer'
   MUSICIAN = 'musician'
   WRITER = 'writer'
   ${NEW_ENTITY_NAME}= '${NEW_ENTITY_NAME}'
   # Works

Then you need to add your database among the supported ones. Just add an entry in the ``DUMP_EXTRACTOR`` dictionary in ``${PROJECT_ROOT}/soweego/importer/importer.py``.

.. code:: py

   DUMP_EXTRACTOR = {
       keys.DISCOGS: DiscogsDumpExtractor,
       keys.IMDB: ImdbDumpExtractor,
       keys.MUSICBRAINZ: MusicBrainzDumpExtractor,
       keys.${NEW_DATABASE}: ${NEW_DATABASE}DumpExtractor
   }
  
The last step is to set up the dictionary ``TARGET_CATALOGS`` in ``${PROJECT_ROOT}/soweego/commons/constants.cs``.
Your entry should be like:

.. code:: py

   keys.MUSICBRAINZ: {
           keys.MUSICIAN: {
               keys.CLASS_QID: vocabulary.MUSICIAN_QID,
               keys.MAIN_ENTITY: MusicbrainzArtistEntity,
               keys.LINK_ENTITY: MusicbrainzArtistLinkEntity,
               keys.NLP_ENTITY: None,
               keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
               keys.WORK_TYPE: keys.MUSICAL_WORK,
           },
           keys.BAND: {
               keys.CLASS_QID: vocabulary.BAND_QID,
               keys.MAIN_ENTITY: MusicbrainzBandEntity,
               keys.LINK_ENTITY: MusicbrainzBandLinkEntity,
               keys.NLP_ENTITY: None,
               keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
               keys.WORK_TYPE: keys.MUSICAL_WORK,
           },
           keys.MUSICAL_WORK: {
               keys.CLASS_QID: vocabulary.MUSICAL_WORK_QID,
               keys.MAIN_ENTITY: MusicbrainzReleaseGroupEntity,
               keys.LINK_ENTITY: MusicbrainzReleaseGroupLinkEntity,
               keys.NLP_ENTITY: None,
               keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
               keys.WORK_TYPE: None,
           },
   },
   keys.${NEW_DATABASE}: {
           keys.${NEW_ENTITY_NAME}: {
               keys.CLASS_QID: vocabulary.MUSICIAN_QID, # Insert the Wikidata class QID corresponding to your entity type
               keys.MAIN_ENTITY: ${NEW_DATABASE}${NEW_ENTITY_NAME}Entity,
               keys.LINK_ENTITY: None,
               keys.NLP_ENTITY: None,
               keys.RELATIONSHIP_ENTITY: None,
               keys.WORK_TYPE: None,
           },
   },

Running the import process
--------------------------

1. Ensure to be in `test or production
   mode <https://github.com/Wikidata/soweego/wiki/How-do-I-test-soweego-on-my-machine%3F>`__.

2. run
   ``python -m soweego importer import ${YOUR_DATABASE_NAME}``
