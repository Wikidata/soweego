#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Getters for supported catalogs constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'

import logging

from soweego.commons import constants, keys
from soweego.wikidata import vocabulary

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
LOGGER = logging.getLogger(__name__)


def supported_targets():
    return constants.TARGET_CATALOGS.keys()


def supported_entities():
    result = []
    for key in constants.TARGET_CATALOGS:
        for k in constants.TARGET_CATALOGS[key].keys():
            result.append(k)
    return list(set(result))


def supported_entities_for_target(target):
    return constants.TARGET_CATALOGS[target].keys()


def get_main_entity(target, entity):
    return constants.TARGET_CATALOGS[target][entity][keys.MAIN_ENTITY]


def get_link_entity(target, entity):
    return constants.TARGET_CATALOGS[target][entity][keys.LINK_ENTITY]


def get_nlp_entity(target, entity):
    return constants.TARGET_CATALOGS[target][entity][keys.NLP_ENTITY]


def get_relationship_entity(target, entity):
    return constants.TARGET_CATALOGS[target][entity][keys.RELATIONSHIP_ENTITY]


def get_work_type(target, entity):
    return constants.TARGET_CATALOGS[target][entity][keys.WORK_TYPE]


def get_class_qid(target, entity):
    return constants.TARGET_CATALOGS[target][entity][keys.CLASS_QID]


def get_person_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[keys.PERSON_PID]


def get_work_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[keys.WORK_PID]


def get_catalog_qid(target):
    return vocabulary.CATALOG_MAPPING.get(target)[keys.CATALOG_QID]


def get_catalog_pid(target, entity):
    entity_type = ENTITY_TYPES.get(entity)
    if entity_type is PERSON:
        return get_person_pid(target)
    elif entity_type is WORK:
        return get_work_pid(target)
    else:
        err_msg = f"""Bad entity: {entity}. It should be one of {set(
            ENTITY_TYPES.keys())}"""
        LOGGER.critical(err_msg)
        raise ValueError(err_msg)
