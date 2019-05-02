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


def available_types_for_target(target):
    return TARGET_CATALOGS[target].keys()


def get_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['entity']


def get_link_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['link_entity']


def get_nlp_entity(target, entity_type):
    return TARGET_CATALOGS[target][entity_type]['nlp_entity']


def get_qid(catalog, target_type):
    catalog_dict = vocabulary.CATALOG_MAPPING.get(catalog)

    if catalog_dict is None:
        return None

    if target_type is None:
        return catalog_dict['default']['qid']
    else:
        if catalog_dict.get(target_type) is not None:
            return catalog_dict[target_type]['qid']
        else:
            return catalog_dict['default']['qid']


def get_pid(catalog, target_type):
    catalog_dict = vocabulary.CATALOG_MAPPING.get(catalog)

    if catalog_dict is None:
        return None

    if target_type is None:
        return catalog_dict['default']['pid']
    else:
        if catalog_dict.get(target_type) is not None:
            return catalog_dict[target_type]['pid']
        else:
            return catalog_dict['default']['pid']
