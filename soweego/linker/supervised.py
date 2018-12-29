#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""TODO module docstring"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging

from pandas import DataFrame

from soweego.commons import data_gathering
from soweego.validator.checks import get_vocabulary

LOGGER = logging.getLogger(__name__)


def build_training_set(entity, catalog):
    catalog_terms = get_vocabulary(catalog)

    # Wikidata
    wikidata = {}

    data_gathering.gather_identifiers(
        entity, catalog, catalog_terms['pid'], wikidata)
    url_pids, ext_id_pids_to_urls = data_gathering.gather_relevant_pids()
    data_gathering.gather_wikidata_training_set(
        wikidata, url_pids, ext_id_pids_to_urls)
    return DataFrame.from_dict(wikidata, orient='index')


if __name__ == "__main__":
    cornice = build_training_set('musician', 'discogs')
    print('daje')
