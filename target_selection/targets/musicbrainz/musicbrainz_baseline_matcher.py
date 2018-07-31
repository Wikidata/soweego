#!/usr/bin/env python3
# coding: utf-8

import json
import os
import csv
from ..common import matching_strategies

# ATTENTION remember to download the dump by running the bash script in the same folder of this script

# Utilities functions
def get_musicbrainz_artists_from_dump(opened_file_dump, label_column_index, id_column_index, total_number_columns):
    """Given an opened musicbrainz(mb) dump, return a dictionary label - mbid"""
    label_musicbrainz = {}
    fieldnames = [i for i in range(0, total_number_columns)]

    musicbrainz_artists = csv.DictReader(opened_file_dump, dialect='excel-tab', fieldnames=fieldnames)

    for row in musicbrainz_artists:
        # Checks if it's a person
        if row[10] == '1' or '\\N':
            label_musicbrainz[row[label_column_index].lower()] = row[id_column_index]
        else:
            print(row[10])

    return label_musicbrainz

# Retrieves the current module directory path
path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)
output_dir_path = '%s/output' % dir_path

if not os.path.exists(output_dir_path):
    os.makedirs(output_dir_path)

# Wikidata sample loading
labels_qid = json.load(open('musicians_wikidata_sample.json'))

# Opens the latest artist dump and creates a dictionary name - id
artists = {}
with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist' % dir_path) as tsvfile:
    artists = get_musicbrainz_artists_from_dump(tsvfile, 2, 0, 20)
    artists.update(get_musicbrainz_artists_from_dump(tsvfile, 3, 0, 20))

with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist_alias' % dir_path) as tsvfile:
    artists.update(get_musicbrainz_artists_from_dump(tsvfile, 2, 1, 16))
    artists.update(get_musicbrainz_artists_from_dump(tsvfile, 7, 1, 16))

json.dump(artists, open('%s/artists.json' % output_dir_path, 'w'), indent=2, ensure_ascii=False)
# Applies a matching strategy
matches = matching_strategies.equal_strings_match((labels_qid, artists))
json.dump(matches, open('%s/equal_strings_matches.json' % output_dir_path, 'w'), indent=2, ensure_ascii=False)
