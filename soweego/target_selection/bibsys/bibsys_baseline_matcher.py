#!/usr/bin/env python3
# coding: utf-8

import json 
from ../common import matching_strategies

def equal_strings_match():
    """Creates the equal strings match output file"""
    # Wikidata sample loading
    wikidata_sample = json.load(open('C:\\Code\\Wikidata.Soweego\\business\\target_selection\\targets\\bibsys\\wikidata_sample.json'))
    bibsys_dictionary = json.load(open('C:\\Code\\Wikidata.Soweego\\business\\target_selection\\targets\\bibsys\\bibsys_dictionary.json'))
    matches = matching_strategies.equal_strings_match([wikidata_sample, bibsys_dictionary])
    json.dump(matches, open('bibsys_equal_strings_matches.json', 'w'), indent=2, ensure_ascii=False)