from soweego.commons.constants import TARGET_CATALOGS
from soweego.wikidata import vocabulary


def available_targets():
    return TARGET_CATALOGS.keys()


def available_types():
    result = []
    for key in TARGET_CATALOGS:
        for k in TARGET_CATALOGS[key].keys():
            result.append(k)
    return list(set(result))


def get_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['entity']


def get_link_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['link_entity']


def get_qid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)['qid']


def get_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)['pid']
