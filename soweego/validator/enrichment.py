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

import click
from sqlalchemy import and_

from soweego.commons import constants, data_gathering, target_database
from soweego.commons.db_manager import DBManager
from soweego.ingestor import wikidata_bot
from soweego.wikidata import vocabulary

LOGGER = logging.getLogger(__name__)


@click.command()
@click.argument('catalog', type=click.Choice(target_database.available_targets()))
@click.argument('entity', type=click.Choice(target_database.available_types()))
@click.option('--upload/--no-upload', default=False, help='Upload links to Wikidata. Default: no.')
@click.option('--sandbox/--no-sandbox', default=False,
              help='Upload to the Wikidata sandbox item Q4115189. Default: no.')
@click.option('-d', '--dir-io', type=click.Path(file_okay=False), default=constants.SHARED_FOLDER,
              help="Input/output directory, default: '%s'." % constants.SHARED_FOLDER)
def works_people_cli(catalog, entity, upload, sandbox, dir_io):
    statements = generate_statements(catalog, entity, dir_io)
    with open(os.path.join(dir_io, constants.WORKS_BY_PEOPLE_STATEMENTS), 'w') as fout:
        for subj, pred, obj in statements:
            fout.write(f'{subj},{pred},{obj}\n')
            if upload and sandbox:
                wikidata_bot.add_or_reference(
                    vocabulary.SANDBOX_1, pred, obj, target_database.get_qid(catalog))
            elif upload:
                wikidata_bot.add_or_reference(
                    subj, pred, obj, target_database.get_qid(catalog))


def generate_statements(catalog, entity, dir_io, page=1000):
    works, people = {}, {}
    claim_pid = vocabulary.WORKS_BY_PEOPLE_MAPPING[catalog][entity]
    # Gather works IDs
    data_gathering.gather_target_ids(
        entity, catalog, target_database.get_work_pid(catalog), works)
    # Gather people IDs
    data_gathering.gather_target_ids(
        entity, catalog, target_database.get_person_pid(catalog), people)
    # Invert & simplify dictionaries for easier lookup later on
    works_inverted, people_inverted = _invert_and_simplify(
        works), _invert_and_simplify(people)
    del works, people
    db_entity = target_database.get_relationship_entity(catalog, entity)
    session = DBManager().connect_to_db()
    try:
        works_to_people = session.query(db_entity).filter(
            and_(
                db_entity.from_catalog_id.in_(works_inverted.keys()),
                db_entity.to_catalog_id.in_(people_inverted.keys())
            )
        ).yield_per(page).enable_eagerloads(False)
        result = ((works_inverted[res.from_catalog_id], claim_pid,
                   people_inverted[res.to_catalog_id]) for res in works_to_people)
    except:
        session.rollback()
        raise
    finally:
        session.close()
    return result


def _invert_and_simplify(dictionary):
    inverted = {}
    for qid, obj in dictionary.items():
        tids = obj[constants.TID]
        for tid in tids:
            qid_already_there = inverted.get(tid)
            if qid_already_there:
                LOGGER.warning(
                    "Skipping QID '%s': there is already QID '%s' for target ID '%s'", qid, qid_already_there, tid)
                continue
            inverted[tid] = qid
    return inverted
