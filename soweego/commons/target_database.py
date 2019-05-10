from soweego.commons import constants
from soweego.wikidata import vocabulary


def available_targets():
    return constants.TARGET_CATALOGS.keys()


def available_types():
    result = []
    for key in constants.TARGET_CATALOGS:
        for k in constants.TARGET_CATALOGS[key].keys():
            result.append(k)
    return list(set(result))


def available_types_for_target(target):
    return constants.TARGET_CATALOGS[target].keys()


def get_main_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][constants.MAIN_ENTITY]


def get_link_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][constants.LINK_ENTITY]


def get_nlp_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][constants.NLP_ENTITY]


def get_relationship_entity(target, entity_type):
    return constants.TARGET_CATALOGS[target][entity_type][constants.RELATIONSHIP_ENTITY]


def get_qid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[constants.QID]


def get_person_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[constants.PERSON_PID]


def get_work_pid(catalog):
    return vocabulary.CATALOG_MAPPING.get(catalog)[constants.WORK_PID]
