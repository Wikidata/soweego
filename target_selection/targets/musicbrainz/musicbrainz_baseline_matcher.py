#!/usr/bin/env python3
# coding: utf-8

import json
import os
import csv
from ..common import matching_strategies

# Utilities functions
def get_musicbrainz_artists_from_dump(opened_file_dump):
    """Given an opened musicbrainz(mb) dump, return a dictionary label - mbid"""
    label_musicbrainz = {}
    fieldnames = [i for i in range(0, 20)]

    musicbrainz_artists = csv.DictReader(opened_file_dump, dialect='excel-tab', fieldnames=fieldnames)

    for row in musicbrainz_artists:
        # Checks if it's a person
        if row[10] == '1' or '\\N':
            label_musicbrainz[row[2].lower()] = row[0]
        else:
            print(row[10])

    return label_musicbrainz

# Retrieves the current module directory path
path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)

# Wikidata sample loading
labels_qid = json.load(open('musicians_wikidata_sample.json'))

# Opens the latest artist dump and creates a dictionary name - id
artists = {}
with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist' % dir_path) as tsvfile:
    artists = get_musicbrainz_artists_from_dump(tsvfile)
    json.dump(artists, open('%s/artists.json' % dir_path, 'w'), indent=2, ensure_ascii=False)

# Applies a matching strategy
matches = matching_strategies.equal_strings_match((labels_qid, artists))
json.dump(matches, open('%s/matches.json' % dir_path, 'w'), indent=2, ensure_ascii=False)
