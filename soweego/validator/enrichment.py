#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Enrichment of Wikidata statements based on identifiers and data of a target catalog"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import logging
import os
from itertools import product

import click
from sqlalchemy import and_
from tqdm import tqdm

from soweego.commons import (constants, data_gathering, keys, target_database,
                             utils)
from soweego.commons.db_manager import DBManager
from soweego.ingestor import wikidata_bot
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.supported_targets()))
@click.argument('entity', type=click.Choice(target_database.supported_entities()))
@click.option('--upload/--no-upload', default=False, help='Upload links to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False,
              help="Upload to the Wikidata sandbox item Q4115189. Use with '--upload'. Default: no.")
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
def works_people_cli(catalog, entity, upload, sandbox, dir_io):
    """Populate statements about works by people."""
    if upload:
        to_upload = set()
    statements = generate_statements(catalog, entity)

    # Boolean to run IMDb-specific checks
    is_imdb = catalog == keys.IMDB
    catalog_qid = target_database.get_catalog_qid(catalog)
    person_pid = target_database.get_person_pid(catalog)

    with open(os.path.join(dir_io, constants.WORKS_BY_PEOPLE_STATEMENTS % (catalog, entity)), 'w', 1) as fout:
        for subj, pred, obj, tid in statements:
            fout.write(f'{subj},{pred},{obj},{tid}\n')
            if sandbox:
                wikidata_bot.add_or_reference_works(
                    vocabulary.SANDBOX_3, pred, obj, catalog_qid, person_pid, tid, is_imdb=is_imdb)
            if upload:
                # Fill a list from the statements generator
                # to prevent lost connections to the SQL DB
                to_upload.add((subj, pred, obj, tid))

    if upload:
        wikidata_bot.add_works_statements(to_upload, catalog, sandbox)


def generate_statements(catalog, entity, bucket_size=5000):
    works, people = {}, {}
    claim_pid = vocabulary.WORKS_BY_PEOPLE_MAPPING[catalog][entity]
    # Gather works IDs
    data_gathering.gather_target_ids(target_database.get_work_type(
        catalog, entity), catalog, target_database.get_work_pid(catalog), works)
    # Gather people IDs
    data_gathering.gather_target_ids(
        entity, catalog, target_database.get_person_pid(catalog), people)
    # Invert & simplify dictionaries for easier lookup later on
    works_inverted, people_inverted = _invert_and_simplify(
        works), _invert_and_simplify(people)
    del works, people
    # Make buckets: more queries, but more efficient ones
    works_buckets = utils.make_buckets(
        list(works_inverted.keys()), bucket_size=bucket_size)
    people_buckets = utils.make_buckets(
        list(people_inverted.keys()), bucket_size=bucket_size)
    total_queries = len(works_buckets) * len(people_buckets)

    LOGGER.info(
        'Firing %d queries to the internal database, this will take a while ...', total_queries)

    db_entity = target_database.get_relationship_entity(catalog, entity)
    session = DBManager().connect_to_db()
    try:
        for works, people in tqdm(product(works_buckets, people_buckets), total=total_queries):
            works_to_people = session.query(db_entity).filter(
                and_(
                    db_entity.from_catalog_id.in_(works),
                    db_entity.to_catalog_id.in_(people)
                )
            )

            for result in works_to_people:
                yield works_inverted[result.from_catalog_id], claim_pid, people_inverted[result.to_catalog_id], result.to_catalog_id
    except:
        session.rollback()
        raise
    finally:
        session.close()

    LOGGER.info('Queries done, statements generated')


def _invert_and_simplify(dictionary):
    inverted = {}
    for qid, obj in dictionary.items():
        tids = obj[keys.TID]
        for tid in tids:
            qid_already_there = inverted.get(tid)
            if qid_already_there:
                LOGGER.warning(
                    'Target ID %s has multiple QIDs. Skipping %s, keeping %s', tid, qid, qid_already_there)
                continue
            inverted[tid] = qid
    return inverted
