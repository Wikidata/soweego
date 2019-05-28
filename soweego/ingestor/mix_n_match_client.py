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
import requests

from soweego.commons import target_database, keys
from soweego.commons.db_manager import DBManager
from soweego.importer.models import mix_n_match

LOGGER = logging.getLogger(__name__)

SUPPORTED_TARGETS = set(target_database.supported_targets()) ^ {keys.TWITTER}

MNM_DB = 's51434__mixnmatch_p'
MNM_API_URL = 'https://tools.wmflabs.org/mix-n-match/api.php'
MNM_API_ACTIVATION_PARAMS = {
    'query': 'update_overview',
    'catalog': None  # To be filled by activate_catalog
}

CATALOG_NOTE = 'Uploaded by soweego'
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
    catalog_id = add_catalog(catalog, entity)
    activate_catalog(catalog_id, catalog, entity)


def add_catalog(catalog, entity):
    name_field = f'{catalog.title()} {entity}'

    session = DBManager(MNM_DB).new_session()
    catalog_id = None
    try:
        existing = session.query(mix_n_match.MnMCatalog).filter_by(name=name_field).first()
        if existing is None:
            LOGGER.info("Adding %s %s catalog to the mix'n'match DB ... ", catalog, entity)
            db_entity = mix_n_match.MnMCatalog()
            _set_catalog_fields(db_entity, name_field, catalog, entity)
            session.add(db_entity)
            session.commit()
            catalog_id = db_entity.id
        else:
            LOGGER.info('Updating %s %s catalog ... ', catalog, entity)
            catalog_id = existing.id
            _set_catalog_fields(existing, name_field, catalog, entity)
            session.add(existing)
            session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

    LOGGER.info('Catalog addition/update went fine. Internal ID: %d', catalog_id)
    return catalog_id


def _set_catalog_fields(db_entity, name_field, catalog, entity):
    db_entity.name = name_field
    db_entity.active = 1
    db_entity.note = CATALOG_NOTE
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


def activate_catalog(catalog_id, catalog, entity):
    MNM_API_ACTIVATION_PARAMS['catalog'] = catalog_id
    activated = requests.get(MNM_API_URL, params=MNM_API_ACTIVATION_PARAMS)

    if activated.ok:
        try:
            json_status = activated.json()
        except ValueError:
            LOGGER.error(
                'Activation of %s %s (catalog ID: %d) failed. '
                'Reason: no JSON response',
                catalog, entity, catalog_id
            )
            LOGGER.debug('Response from %s: %s', activated.url, activated.text)
            return

        if json_status.get('status') == 'OK':
            LOGGER.info(
                '%s %s (catalog ID: %d) successfully activated',
                catalog, entity, catalog_id
            )
        else:
            LOGGER.error(
                'Activation of %s %s (catalog ID: %d) failed. Reason: %s',
                catalog, entity, catalog_id, json_status
            )
    else:
        LOGGER.error(
            'Activation request for %s %s (catalog ID: %d) '
            'failed with HTTP error code %d',
            catalog, entity, catalog_id, activated.status_code
        )
