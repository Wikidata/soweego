#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""A client that uploads non-confident links
to the `Mix'n'match <https://meta.wikimedia.org/wiki/Mix%27n%27match/Manual>`_ tool for curation.

It inserts data in the ``catalog`` and ``entry`` tables of the ``s51434__mixnmatch_p``
database located in `ToolsDB <https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#User_databases>`_ under the Wikimedia `Toolforge <https://wikitech.wikimedia.org/wiki/Portal:Toolforge>`_ infrastructure.
See `how to connect <https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database#Connecting_to_the_database_replicas>`_.
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import logging
import sys
from datetime import datetime
from typing import Tuple

import click
import requests
from pandas import read_csv
from soweego.commons import keys, target_database
from soweego.commons.constants import SUPPORTED_ENTITIES
from soweego.commons.db_manager import DBManager
from soweego.importer.models import mix_n_match
from soweego.wikidata.vocabulary import HUMAN_QID
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

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
@click.argument('matches', type=click.Path(exists=True, dir_okay=False))
def cli(catalog, entity, confidence_range, matches):
    """Upload matches to the Mix'n'match tool.

    CONFIDENCE_RANGE must be a pair of floats
    that indicate the minimum and maximum confidence scores.

    MATCHES must be a CSV file path.
    Format: QID, catalog_identifier, confidence_score

    The CSV file can be compressed.

    Example:

    echo Q446627,266995,0.666 > rhell.csv

    python -m soweego ingest mnm discogs musician 0.3 0.7 rhell.csv

    Result: see 'Latest catalogs' at https://tools.wmflabs.org/mix-n-match/
    """
    catalog_id = add_catalog(catalog, entity)
    if catalog_id is None:
        sys.exit(1)

    add_matches(matches, catalog_id, catalog, entity, confidence_range)
    activate_catalog(catalog_id, catalog, entity)


def add_catalog(catalog: str, entity: str) -> int:
    """Add or update a catalog.

    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :return: the catalog *id* field of the *catalog* table
      in the *s51434__mixnmatch_p* Toolforge database
    """
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
            "'-l soweego.ingester DEBUG' for more details",
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


def add_matches(
    file_path: str,
    catalog_id: int,
    catalog: str,
    entity: str,
    confidence_range: Tuple[float, float],
) -> None:
    """Add or update matches to an existing catalog.
    Curated matches found in the catalog are kept as is.

    :param file_path: path to a file with matches
    :param catalog_id: the catalog *id* field of the *catalog* table
      in the *s51434__mixnmatch_p* Toolforge database
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    :param confidence_range: a pair of floats indicating
      the minimum and maximum confidence scores of matches
      that will be added/updated.
    """
    success = True  # Flag to log that everything went fine
    class_qid, url_prefix = _handle_metadata(catalog, entity)
    matches = _handle_matches(file_path, confidence_range)

    LOGGER.info(
        "Starting import of %s %s matches (catalog ID: %d) into the mix'n'match DB ...",
        catalog,
        entity,
        catalog_id,
    )

    start = datetime.now()
    session = DBManager(MNM_DB).new_session()

    # Note that the session is kept open after this operation
    curated, success = _sync_matches(session, catalog_id, success)

    # Filter curated matches:
    # rows with tids that are NOT (~) in curated tids
    matches = matches[~matches[keys.TID].isin(curated)]

    n_matches = len(matches)
    matches_reader = matches.itertuples(index=False, name=None)
    batch = []

    try:
        _import_matches(
            batch,
            catalog,
            catalog_id,
            class_qid,
            entity,
            matches_reader,
            n_matches,
            session,
            url_prefix,
        )

        LOGGER.info(
            'Adding last batch of %d %s %s matches, this may take a while ...',
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
            "'-l soweego.ingester DEBUG' for more details",
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
            'Import of %s %s matches (catalog ID: %d) completed in %s. '
            'Total matches: %d',
            catalog,
            entity,
            catalog_id,
            end - start,
            n_matches,
        )


def _import_matches(
    batch,
    catalog,
    catalog_id,
    class_qid,
    entity,
    links_reader,
    n_links,
    session,
    url_prefix,
):
    for qid, tid, score in tqdm(links_reader, total=n_links):
        url = '' if url_prefix is None else f'{url_prefix}{tid}'

        db_entity = mix_n_match.MnMEntry()
        _set_entry_fields(
            db_entity, catalog_id, qid, tid, url, class_qid, score
        )
        batch.append(db_entity)

        if len(batch) >= COMMIT_EVERY:
            LOGGER.info(
                'Adding batch of %d %s %s matches, this may take a while ...',
                COMMIT_EVERY,
                catalog,
                entity,
            )
            session.bulk_save_objects(batch)
            session.commit()

            # Clear session & batch entities
            session.expunge_all()
            batch.clear()


def _sync_matches(session, catalog_id, success):
    curated = []
    try:
        curated = session.query(mix_n_match.MnMEntry.ext_id).filter(
            mix_n_match.MnMEntry.catalog == catalog_id,
            mix_n_match.MnMEntry.user != 0,
        )
        # Result is a tuple: (tid,)
        curated = [res[0] for res in curated]

        n_deleted = _delete_non_curated_matches(catalog_id, session)

        session.commit()
        session.expunge_all()

        LOGGER.info(
            'Kept %d curated matches, deleted %d remaining matches',
            len(curated),
            n_deleted,
        )
    except SQLAlchemyError as error:
        LOGGER.error(
            "Failed query of existing matches due to %s. "
            "You can enable the debug log with the CLI option "
            "'-l soweego.ingester DEBUG' for more details",
            error.__class__.__name__,
        )
        LOGGER.debug(error)

        session.rollback()
        success = False
    return curated, success


def _delete_non_curated_matches(catalog_id, session):
    n_deleted = (
        session.query(mix_n_match.MnMEntry)
        .filter(
            mix_n_match.MnMEntry.catalog == catalog_id,
            mix_n_match.MnMEntry.user == 0,
        )
        .delete(synchronize_session=False)
    )
    return n_deleted


def _handle_matches(file_path, confidence_range):
    links = read_csv(file_path, names=INPUT_CSV_HEADER)
    # Filter links above threshold,
    # sort by confidence in ascending order,
    # drop duplicate TIDs,
    # keep the duplicate with the best score (last one)
    links = (
        links[
            (links[keys.CONFIDENCE] >= confidence_range[0])
            & (links[keys.CONFIDENCE] <= confidence_range[1])
        ]
        .sort_values(keys.CONFIDENCE)
        .drop_duplicates(keys.TID, keep='last')
    )
    return links


def _handle_metadata(catalog, entity):
    url_prefix = CATALOG_ENTITY_URLS.get(f'{catalog}_{entity}')
    if url_prefix is None:
        LOGGER.debug('URL not available for %s %s', catalog, entity)
    # Set human as default when the relevant class QID
    # is not an instance-of (P31) value
    if SUPPORTED_ENTITIES.get(entity) == keys.CLASS_QUERY:
        class_qid = target_database.get_class_qid(catalog, entity)
    else:
        class_qid = HUMAN_QID
    return class_qid, url_prefix


def activate_catalog(catalog_id: int, catalog: str, entity: str) -> None:
    """Activate a catalog.

    :param catalog_id: the catalog *id* field of the *catalog* table
      in the *s51434__mixnmatch_p* Toolforge database
    :param catalog: ``{'discogs', 'imdb', 'musicbrainz', 'twitter'}``.
      A supported catalog
    :param entity: ``{'actor', 'band', 'director', 'musician', 'producer',
      'writer', 'audiovisual_work', 'musical_work'}``.
      A supported entity
    """
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


def _set_catalog_fields(db_entity, name_field, catalog, entity):
    db_entity.name = name_field
    db_entity.active = 1
    db_entity.note = NOTE_FIELD
    db_entity.type = CATALOG_TYPES.get(catalog, '')
    db_entity.source_item = int(
        target_database.get_catalog_qid(catalog).lstrip('Q')
    )
    wd_prop = target_database.get_catalog_pid(catalog, entity)
    db_entity.wd_prop = int(wd_prop.lstrip('P'))
    db_entity.search_wp = SEARCH_WP_FIELD


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
