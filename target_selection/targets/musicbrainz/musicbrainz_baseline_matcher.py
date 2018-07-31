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

def get_path():
    """Return the path of the current module"""
    path = os.path.abspath(__file__)
    return os.path.dirname(path)

def get_output_path():
    """Returns the path of the output files"""
    output_dir_path = '%s/output' % get_path()

    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    return output_dir_path

def get_label_musicbrainzid_dict():
    """Returns the name-musicbrainzid dictionary. If it's not stored, creates a brand new file"""
    filepath = '%s/artists.json' % get_output_path()
    if os.path.isfile(filepath):
        return json.load(open(filepath))
    else:
        artists = {}
        with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist' % get_path()) as tsvfile:
            artists = get_musicbrainz_artists_from_dump(tsvfile, 2, 0, 20)
            artists.update(get_musicbrainz_artists_from_dump(tsvfile, 3, 0, 20))

        with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist_alias' % get_path()) as tsvfile:
            artists.update(get_musicbrainz_artists_from_dump(tsvfile, 2, 1, 16))
            artists.update(get_musicbrainz_artists_from_dump(tsvfile, 7, 1, 16))

        json.dump(artists, open('%s/artists.json' % get_output_path(), 'w'), indent=2, ensure_ascii=False)
        return artists

def equal_strings_match():
    """Creates the equal strings match output file"""
    # Wikidata sample loading
    labels_qid = json.load(open('musicians_wikidata_sample.json'))
    matches = matching_strategies.equal_strings_match((labels_qid, get_label_musicbrainzid_dict()))
    json.dump(matches, open('%s/equal_strings_matches.json' % get_output_path(), 'w'), indent=2, ensure_ascii=False)
