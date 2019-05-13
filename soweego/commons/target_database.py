#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Getters for supported catalogs constants"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2019, Hjfocs'


from soweego.commons import constants, keys
from soweego.wikidata import vocabulary


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


def get_main_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][keys.MAIN_ENTITY]


def get_link_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][keys.LINK_ENTITY]


def get_nlp_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][keys.NLP_ENTITY]


def get_relationship_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][keys.RELATIONSHIP_ENTITY]


def get_person_qid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[keys.PERSON_QID]


def get_work_qid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[keys.WORK_QID]


def get_person_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[keys.PERSON_PID]


def get_work_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[keys.WORK_PID]
