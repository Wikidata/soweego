.. _new:

Import a new catalog
====================

Five steps:

1. set up the :ref:`dev`
2. `declare <https://docs.sqlalchemy.org/en/13/orm/tutorial.html#declare-a-mapping>`_
   the `SQLAlchemy <https://www.sqlalchemy.org/>`_
   Object Relational Mapper
   (:ref:`orm`)
3. implement the catalog :ref:`extractor`
4. :ref:`cli`
5. :ref:`run`

.. note::

   you will encounter some variables while reading this page

   - set ``${PROJECT_ROOT}`` to the root directory where *soweego* lives
   - set ``${CATALOG}`` to the name of the catalog you want to import, like ``IMDb``
   - set ``${ENTITY}`` to what the catalog is about, like ``Musician`` or ``Book``
   - the other ones should be self-explanatory


.. _orm:

ORM
---

1. create a Python file in::

   ${PROJECT_ROOT}/soweego/importer/models/${CATALOG}_entity.py

2. paste the code snippet below
3. set the ``${...}`` variables accordingly
4. **optional:** define catalog-specific attributes

   - see ``TODO`` in the code snippet
   - just remember that attribute names **must be different** from
     :class:`~soweego.importer.models.base_entity.BaseEntity` ones,
     otherwise you would override them
   - don't forget their documentation!

.. code-block::
   :force:

   #!/usr/bin/env python3
   # -*- coding: utf-8 -*-

   """`${CATALOG} <${CATALOG_HOME_URL}>`_
   `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities."""

   __author__ = '${YOUR_NAME_HERE}'
   __email__ = '${YOUR_EMAIL_HERE}'
   __version__ = '1.0'
   __license__ = 'GPL-3.0'
   __copyright__ = 'Copyleft ${YEAR}, ${YOUR_NAME_HERE}'

   from sqlalchemy import Column, String

   from soweego.importer.models.base_entity import BaseEntity

   ${ENTITY}_TABLE = '${CATALOG}_${ENTITY}'

   class ${CATALOG}${ENTITY}Entity(BaseEntity):
       """A ${CATALOG} ${ENTITY}.
       It comes from the ${CATALOG_DUMP_FILE} dataset.
       See the `download page <${CATALOG_DOWNLOAD_URL}>`_.

       **Attributes:**

       - **birth_place** (string(255)) - a birth place

       """

       __tablename__ = ${ENTITY}_TABLE
       __mapper_args__ = {
           'polymorphic_identity': __tablename__,
           'concrete': True
       }

       # TODO Optional: define catalog-specific attributes here
       # For instance:
       birth_place = Column(String(255))


.. _extractor:

Extractor
---------

1. create a Python file in::

   ${PROJECT_ROOT}/soweego/importer/${CATALOG}_dump_extractor.py

2. paste the code snippet below
3. set the ``${...}`` variables accordingly
4. implement
   :class:`~soweego.importer.base_dump_extractor.BaseDumpExtractor` methods:

   -  :meth:`~soweego.importer.base_dump_extractor.BaseDumpExtractor.extract_and_populate`
      should extract instances of your ``${CATALOG}${ENTITY}Entity``
      from relevant catalog dumps and store them in a database.
      The ``extract`` step is up to you.
      For the ``populate`` step, see :ref:`populate`
   -  :meth:`~soweego.importer.base_dump_extractor.BaseDumpExtractor.get_dump_download_urls`
      should compute the latest list of URLs to download catalog dumps.
      Tipically, there will be only one, but you never know
   
5. still tortured by doubts? Check out
   :class:`~soweego.importer.discogs_dump_extractor.DiscogsDumpExtractor`,
   :class:`~soweego.importer.imdb_dump_extractor.IMDbDumpExtractor`,
   or
   :class:`~soweego.importer.discogs_dump_extractor.MusicBrainzDumpExtractor`.
   You are now doubtless

.. code-block::
   :force:

   #!/usr/bin/env python3
   # -*- coding: utf-8 -*-

   """`${CATALOG} <${CATALOG_HOME_URL}>`_
   `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities."""

   __author__ = '${YOUR_NAME_HERE}'
   __email__ = '${YOUR_EMAIL_HERE}'
   __version__ = '1.0'
   __license__ = 'GPL-3.0'
   __copyright__ = 'Copyleft ${YEAR}, ${YOUR_NAME_HERE}'

   from soweego.importer.base_dump_extractor import BaseDumpExtractor


   class ${CATALOG}DumpExtractor(BaseDumpExtractor):
       """Download ${CATALOG} dumps, extract data, and
       populate a database instance.
       """

       def extract_and_populate(
               self, dump_file_paths: List[str], resolve: bool
       ) -> None:
           # TODO implement!

       def get_dump_download_urls(self) -> Optional[List[str]]:
           # TODO implement!


.. _populate:

Populate the SQL database
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block::
   :force:

   from sqlalchemy.exc import SQLAlchemyError

   from soweego.commons.db_manager import DBManager
   from soweego.importer.base_dump_extractor import BaseDumpExtractor


   class ${CATALOG}DumpExtractor(BaseDumpExtractor):

      def extract_and_populate(
              self, dump_file_paths: List[str], resolve: bool
      ) -> None:

          # The `extract` step should build a list of entities
          # For instance:
          entities = _extract_from(dump_file_paths)

          # 1. Get a `DBManager` instance
          db_manager = DBManager()

          # 2. Drop & recreate database tables
          db_manager.drop(${CATALOG}${ENTITY})
          db_manager.create(${CATALOG}${ENTITY})

          # 3. Create a session, AKA a database transaction
          session = db_manager.new_session()

          try:
              # 4. Add a list of entities to the session
              session.bulk_save_objects(entities)

              # 5. Commit the session
              session.commit()

          except SQLAlchemyError as error:
              # 6. Handle transaction errors
              # For instance: (are you serious? Don't do this)
              print(f'There was an error: {error}')

              session.rollback()

          finally:
              session.close()


.. _cli:

Set up the CLI to import your catalog
-------------------------------------

1. add your catalog keys in ::

   ${PROJECT_ROOT}/soweego/commons/keys.py

.. code-block::
   :force:

   # Supported catalogs
   MUSICBRAINZ = 'musicbrainz'
   ...
   ${CATALOG} = '${CATALOG}'

   # Supported entities
   # People
   ACTOR = 'actor'
   ...
   ${ENTITY} = '${ENTITY}'

2. include your extractor in the ``DUMP_EXTRACTOR`` dictionary of ::

   ${PROJECT_ROOT}/soweego/importer/importer.py

.. code-block::
   :force:

   DUMP_EXTRACTOR = {
       keys.MUSICBRAINZ: MusicBrainzDumpExtractor,
       ...
       keys.${CATALOG}: ${CATALOG}DumpExtractor
   }

3. add the Wikidata class QID corresponding to your entity in ::

   ${PROJECT_ROOT}/soweego/wikidata/vocabulary.py

.. code-block::
   :force:

   # Class QID of supported entities
   # People
   ACTOR_QID = 'Q33999'
   ...
   ${ENTITY}_QID = '${QID}'

4. include your catalog mapping in the ``TARGET_CATALOGS`` dictionary of ::

   ${PROJECT_ROOT}/soweego/commons/constants.py

.. code-block::
   :force:

   keys.MUSICBRAINZ: {
           keys.MUSICIAN: {
               keys.CLASS_QID: vocabulary.MUSICIAN_QID,
               keys.MAIN_ENTITY: MusicBrainzArtistEntity,
               keys.LINK_ENTITY: MusicBrainzArtistLinkEntity,
               keys.NLP_ENTITY: None,
               keys.RELATIONSHIP_ENTITY: MusicBrainzReleaseGroupArtistRelationship,
               keys.WORK_TYPE: keys.MUSICAL_WORK,
           },
           ...
   },
   keys.${CATALOG}: {
           keys.${ENTITY}: {
               keys.CLASS_QID: vocabulary.${ENTITY}_QID,
               keys.MAIN_ENTITY: ${CATALOG}${ENTITY}Entity,
               keys.LINK_ENTITY: None,
               keys.NLP_ENTITY: None,
               keys.RELATIONSHIP_ENTITY: None,
               keys.WORK_TYPE: None,
           },
   },


.. _run:

Run the importer
----------------

.. code-block:: text

   :/app/soweego# python -m soweego importer import ${CATALOG}
