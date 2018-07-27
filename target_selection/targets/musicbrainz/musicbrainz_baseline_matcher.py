#!/usr/bin/env python3
# coding: utf-8

import json
import os
import csv

# Wikidata sample loading
labels_qid = json.load(open('../../wikidata_sample.json'))

# Opens the latest artist dump and creates a dictionary name - id
label_musicbrainz = {}
fieldnames = [i for i in range(0, 20)]
with open('/Users/maxfrax/Desktop/soweego/target_selection/targets/musicbrainz/musicbrainz_dump_20180725-001823/mbdump/artist') as tsvfile:
    musicbrainz_artists = csv.DictReader(
        tsvfile, dialect='excel-tab', fieldnames=fieldnames)
    for row in musicbrainz_artists:
        # Checks if it's a person
        if row[10] == '1' or '\\N':
            label_musicbrainz[row[2].lower()] = row[0]
        else:
            print(row[10])
json.dump(label_musicbrainz, open('artists.json', 'w'),
          indent=2, ensure_ascii=False)