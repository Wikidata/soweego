from soweego.commons.constants import TARGET_CATALOGS
from soweego.wikidata import vocabulary


def available_targets():
    return TARGET_CATALOGS.keys()


def available_types():
    result = set()
    for key in TARGET_CATALOGS:
        for k in TARGET_CATALOGS[key].keys():
            result.add(k)
    return list(result)


def available_types_for_target(target):
    return TARGET_CATALOGS[target].keys()


def get_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['entity']


def get_link_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['link_entity']


def get_nlp_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['nlp_entity']


def get_qid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)['qid']


def get_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)['pid']
