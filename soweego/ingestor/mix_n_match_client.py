#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A client that uploads non-confident links
to the Mix'n'match database for curation.

It inserts data in the ``catalog`` and ``entry`` tables of the ``s51434__mixnmatch_p``
database located in ToolsDB under the Wikimedia Toolforge infrastructure.
See https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Connecting_to_the_database_replicas
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import logging

import click

from soweego.commons import target_database, keys
from soweego.commons.db_manager import DBManager
from soweego.importer.models import mix_n_match

LOGGER = logging.getLogger(__name__)

SUPPORTED_TARGETS = set(target_database.supported_targets()) ^ {keys.TWITTER}
MNM_DB = 's51434__mixnmatch_p'
CATALOG_DESCRIPTION = 'Uploaded by soweego'
SEARCH_WP_FIELD = 'en'
CINEMA = 'cinema'
MUSIC = 'music'
SOCIAL = 'social'
CATALOG_TYPES = {
    keys.DISCOGS: MUSIC,
    keys.IMDB: CINEMA,
    keys.MUSICBRAINZ: MUSIC,
    keys.TWITTER: SOCIAL
}

PERSON = 'person'
WORK = 'work'
ENTITY_TYPES = {
    keys.ACTOR: PERSON,
    keys.BAND: PERSON,
    keys.DIRECTOR: PERSON,
    keys.MUSICIAN: PERSON,
    keys.PRODUCER: PERSON,
    keys.WRITER: PERSON,
    keys.MUSICAL_WORK: WORK,
    keys.AUDIOVISUAL_WORK: WORK
}


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.argument('links', type=click.File())
def cli(catalog, entity, links):
    """Upload identifiers to the mix'n'match tool.

    LINKS must be a QID, catalog_identifier CSV file.
    """
    # FIXME inferire catalog ed entity dal nome del file discogs_musician_linker_result.csv.gz
    add_catalog(catalog, entity)


def add_catalog(catalog, entity):
    name_field = f'{catalog.title()} {entity}'

    import ipdb; ipdb.set_trace()
    session = DBManager(MNM_DB).new_session()
    try:
        existing = session.query(mix_n_match.MnMCatalog).filter_by(name=name_field).first()
        if existing is None:
            LOGGER.info('Inserting %s %s catalog metadata ... ', catalog, entity)
            db_entity = mix_n_match.MnMCatalog()
            _set_catalog_fields(db_entity, name_field, catalog, entity)
            session.add(db_entity)
            session.commit()
        else:
            LOGGER.info('Updating %s %s catalog metadata ... ', catalog, entity)
            _set_catalog_fields(existing, name_field, catalog, entity)
            session.add(existing)
            session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    LOGGER.info('Catalog metadata insertion/update went fine')


def _set_catalog_fields(db_entity, name_field, catalog, entity):
    db_entity.name = name_field
    db_entity.active = 1
    db_entity.desc = CATALOG_DESCRIPTION
    db_entity.type = CATALOG_TYPES.get(catalog, '')
    db_entity.source_item = int(target_database.get_catalog_qid(catalog).lstrip('Q'))
    entity_type = ENTITY_TYPES.get(entity)
    if entity_type is PERSON:
        wd_prop = target_database.get_person_pid(catalog)
    # Handling other values is not required, since click.Choice already does the job
    elif entity_type is WORK:
        wd_prop = target_database.get_work_pid(catalog)
    db_entity.wd_prop = int(wd_prop.lstrip('P'))
    db_entity.search_wp = SEARCH_WP_FIELD


def add_links(catalog, links):
    # TODO inserire/aggiornare links in tabella entry
    pass


def activate_catalog(catalog):
    # TODO mandare la richiesta HTTP per attivare il catalogo
    pass

