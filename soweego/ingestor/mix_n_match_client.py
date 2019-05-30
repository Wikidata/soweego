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
from datetime import datetime
from sys import exit

import click
import requests
from pandas import read_csv
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from soweego.commons import keys, target_database
from soweego.commons.constants import SUPPORTED_ENTITIES
from soweego.commons.db_manager import DBManager
from soweego.importer.models import mix_n_match
from soweego.wikidata.vocabulary import HUMAN_QID

LOGGER = logging.getLogger(__name__)

SUPPORTED_TARGETS = set(target_database.supported_targets()) ^ {keys.TWITTER}
INPUT_CSV_HEADER = (keys.QID, keys.TID, keys.CONFIDENCE)
COMMIT_EVERY = 10_000  # DB entity batch size

MNM_DB = 's51434__mixnmatch_p'
MNM_API_URL = 'https://tools.wmflabs.org/mix-n-match/api.php'
MNM_API_ACTIVATION_PARAMS = {
    'query': 'update_overview',
    'catalog': None,  # To be filled by activate_catalog
}

TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'  # 20190528131053
NOTE_FIELD = 'Uploaded by soweego'
SEARCH_WP_FIELD = 'en'
EXT_DESC_FIELD = 'soweego confidence score: {}'
USER_FIELD = 0  # stands for 'automatically matched'
CINEMA = 'Cinema'
MUSIC = 'Music'
SOCIAL = 'Social network'

ARTIST = 'artist'
MASTER = 'master'
NAME = 'name'
RELEASE_GROUP = 'release-group'
TITLE = 'title'

DISCOGS_BASE_URL = 'https://www.discogs.com'
IMDB_BASE_URL = 'https://www.imdb.com'
MUSICBRAINZ_BASE_URL = 'https://musicbrainz.org'
DISCOGS_PERSON_URL = f'{DISCOGS_BASE_URL}/{ARTIST}/'
DISCOGS_WORK_URL = f'{DISCOGS_BASE_URL}/{MASTER}/'
IMDB_PERSON_URL = f'{IMDB_BASE_URL}/{NAME}/'
IMDB_WORK_URL = f'{IMDB_BASE_URL}/{TITLE}/'
MUSICBRAINZ_PERSON_URL = f'{MUSICBRAINZ_BASE_URL}/{ARTIST}/'
MUSICBRAINZ_WORK_URL = f'{MUSICBRAINZ_BASE_URL}/{RELEASE_GROUP}/'
TWITTER_URL = 'https://twitter.com/'

CATALOG_TYPES = {
    keys.DISCOGS: MUSIC,
    keys.IMDB: CINEMA,
    keys.MUSICBRAINZ: MUSIC,
    keys.TWITTER: SOCIAL,
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
    keys.AUDIOVISUAL_WORK: WORK,
}

CATALOG_ENTITY_URLS = {
    f'{keys.DISCOGS}_{keys.MUSICIAN}': DISCOGS_PERSON_URL,
    f'{keys.DISCOGS}_{keys.BAND}': DISCOGS_PERSON_URL,
    f'{keys.DISCOGS}_{keys.MUSICAL_WORK}': DISCOGS_WORK_URL,
    f'{keys.MUSICBRAINZ}_{keys.MUSICIAN}': MUSICBRAINZ_PERSON_URL,
    f'{keys.MUSICBRAINZ}_{keys.BAND}': MUSICBRAINZ_PERSON_URL,
    f'{keys.MUSICBRAINZ}_{keys.MUSICAL_WORK}': MUSICBRAINZ_WORK_URL,
    f'{keys.IMDB}_{keys.ACTOR}': IMDB_PERSON_URL,
    f'{keys.IMDB}_{keys.DIRECTOR}': IMDB_PERSON_URL,
    f'{keys.IMDB}_{keys.MUSICIAN}': IMDB_PERSON_URL,
    f'{keys.IMDB}_{keys.PRODUCER}': IMDB_PERSON_URL,
    f'{keys.IMDB}_{keys.WRITER}': IMDB_PERSON_URL,
    f'{keys.IMDB}_{keys.AUDIOVISUAL_WORK}': IMDB_WORK_URL,
    keys.TWITTER: TWITTER_URL,
}


@click.command()
@click.argument('catalog', type=click.Choice(SUPPORTED_TARGETS))
@click.argument(
    'entity', type=click.Choice(target_database.supported_entities())
)
@click.argument('confidence_range', type=(float, float))
@click.argument('links', type=click.Path(exists=True, dir_okay=False))
def cli(catalog, entity, confidence_range, links):
    """Upload identifiers to the mix'n'match tool.

    LINKS must be a CSV file path, with format:
    QID, catalog_identifier, confidence_score.

    The CSV file can come compressed.
    """
    catalog_id = add_catalog(catalog, entity)
    if catalog_id is None:
        exit(1)

    add_links(links, catalog_id, catalog, entity, confidence_range)
    activate_catalog(catalog_id, catalog, entity)


def add_catalog(catalog, entity):
    name_field = f'{catalog.title()} {entity}'

    session = DBManager(MNM_DB).new_session()
    try:
        existing = (
            session.query(mix_n_match.MnMCatalog)
            .filter_by(name=name_field)
            .first()
        )
        if existing is None:
            LOGGER.info(
                "Adding %s %s catalog to the mix'n'match DB ... ",
                catalog,
                entity,
            )
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
    except SQLAlchemyError as error:
        LOGGER.error(
            "Failed catalog addition/update due to %s. "
            "You can enable the debug log with the CLI option "
            "'-l soweego.ingestor DEBUG' for more details",
            error.__class__.__name__,
        )
        LOGGER.debug(error)
        session.rollback()
        return None
    finally:
        session.close()

    LOGGER.info(
        'Catalog addition/update went fine. Internal ID: %d', catalog_id
    )
    return catalog_id


def _set_catalog_fields(db_entity, name_field, catalog, entity):
    db_entity.name = name_field
    db_entity.active = 1
    db_entity.note = NOTE_FIELD
    db_entity.type = CATALOG_TYPES.get(catalog, '')
    db_entity.source_item = int(
        target_database.get_catalog_qid(catalog).lstrip('Q')
    )
    entity_type = ENTITY_TYPES.get(entity)
    if entity_type is PERSON:
        wd_prop = target_database.get_person_pid(catalog)
    # Handling other values is not required, since click.Choice already does the job
    elif entity_type is WORK:
        wd_prop = target_database.get_work_pid(catalog)
    db_entity.wd_prop = int(wd_prop.lstrip('P'))
    db_entity.search_wp = SEARCH_WP_FIELD


def add_links(file_path, catalog_id, catalog, entity, confidence_range):
    success = True  # Flag to log that everything went fine
    url_prefix = CATALOG_ENTITY_URLS.get(f'{catalog}_{entity}')
    if url_prefix is None:
        LOGGER.debug('URL not available for %s %s', catalog, entity)
    # Set human as default when the relevant class QID
    # is not an instance-of (P31) value
    if SUPPORTED_ENTITIES.get(entity) == keys.CLASS_QUERY:
        class_qid = target_database.get_class_qid(catalog, entity)
    else:
        class_qid = HUMAN_QID

    links = read_csv(file_path, names=INPUT_CSV_HEADER)
    # Filter links above threshold,
    # sort by confidence in ascending order,
    # drop duplicate TIDs,
    # keep the duplicate with the best score (last one)
    links = (
        links[(links[keys.CONFIDENCE] >= confidence_range[0]) & (links[keys.CONFIDENCE] <= confidence_range[1])]
        .sort_values(keys.CONFIDENCE)
        .drop_duplicates(keys.TID, keep='last')
    )

    LOGGER.info(
        "Starting import of %s %s links (catalog ID: %d) into the mix'n'match DB ...",
        catalog,
        entity,
        catalog_id,
    )
    start = datetime.now()
    session = DBManager(MNM_DB).new_session()
    curated = []

    # Note that the session is kept open after these operations
    try:
        curated = session.query(mix_n_match.MnMEntry.ext_id).filter(
            mix_n_match.MnMEntry.catalog == catalog_id,
            mix_n_match.MnMEntry.user != 0,
        )
        # Result is a tuple: (tid,)
        curated = [res[0] for res in curated]

        n_deleted = (
            session.query(mix_n_match.MnMEntry)
            .filter(
                mix_n_match.MnMEntry.catalog == catalog_id,
                mix_n_match.MnMEntry.user == 0,
            )
            .delete(synchronize_session=False)
        )

        session.commit()
        session.expunge_all()

        LOGGER.info(
            'Kept %d curated links, deleted %d remaining links',
            len(curated),
            n_deleted,
        )
    except SQLAlchemyError as error:
        LOGGER.error(
            "Failed query of existing links due to %s. "
            "You can enable the debug log with the CLI option "
            "'-l soweego.ingestor DEBUG' for more details",
            error.__class__.__name__,
        )
        LOGGER.debug(error)

        session.rollback()
        success = False

    # Filter curated links:
    # rows with tids that are NOT (~) in curated tids
    links = links[~links[keys.TID].isin(curated)]

    n_links = len(links)
    links_reader = links.itertuples(index=False, name=None)

    batch = []

    try:
        for qid, tid, score in tqdm(links_reader, total=n_links):
            url = '' if url_prefix is None else url_prefix + tid

            db_entity = mix_n_match.MnMEntry()
            _set_entry_fields(
                db_entity, catalog_id, qid, tid, url, class_qid, score
            )
            batch.append(db_entity)

            if len(batch) >= COMMIT_EVERY:
                LOGGER.info(
                    'Adding batch of %d %s %s links, this may take a while ...',
                    COMMIT_EVERY,
                    catalog,
                    entity,
                )
                session.bulk_save_objects(batch)
                session.commit()

                # Clear session & batch entities
                session.expunge_all()
                batch.clear()

        LOGGER.info(
            'Adding last batch of %d %s %s links, this may take a while ...',
            len(batch),
            catalog,
            entity,
        )
        # Commit remaining entities
        session.bulk_save_objects(batch)
        session.commit()

    except SQLAlchemyError as error:
        LOGGER.error(
            "Failed addition/update due to %s. "
            "You can enable the debug log with the CLI option "
            "'-l soweego.ingestor DEBUG' for more details",
            error.__class__.__name__,
        )
        LOGGER.debug(error)
        session.rollback()
        success = False

    finally:
        session.close()

    if success:
        end = datetime.now()
        LOGGER.info(
            'Import of %s %s links (catalog ID: %d) completed in %s. '
            'Total links: %d',
            catalog,
            entity,
            catalog_id,
            end - start,
            n_links,
        )


def _set_entry_fields(
    db_entity, catalog_id, qid, tid, url, class_qid, confidence_score
):
    db_entity.catalog = catalog_id
    db_entity.q = int(qid.lstrip('Q'))
    db_entity.ext_id = tid
    db_entity.ext_name = tid
    db_entity.ext_url = url
    db_entity.type = class_qid
    db_entity.ext_desc = EXT_DESC_FIELD.format(confidence_score)
    db_entity.user = USER_FIELD
    db_entity.timestamp = datetime.now().strftime(TIMESTAMP_FORMAT)


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
                catalog,
                entity,
                catalog_id,
            )
            LOGGER.debug('Response from %s: %s', activated.url, activated.text)
            return

        if json_status.get('status') == 'OK':
            LOGGER.info(
                '%s %s (catalog ID: %d) successfully activated',
                catalog,
                entity,
                catalog_id,
            )
        else:
            LOGGER.error(
                'Activation of %s %s (catalog ID: %d) failed. Reason: %s',
                catalog,
                entity,
                catalog_id,
                json_status,
            )
    else:
        LOGGER.error(
            'Activation request for %s %s (catalog ID: %d) '
            'failed with HTTP error code %d',
            catalog,
            entity,
            catalog_id,
            activated.status_code,
        )
