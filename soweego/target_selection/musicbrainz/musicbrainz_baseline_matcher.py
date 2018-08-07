#!/usr/bin/env python3
# coding: utf-8

import click
import json
import os
import csv
import re
from . import common
from collections import defaultdict
from soweego.target_selection.common import matching_strategies

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

def get_label_musicbrainzid_dict():
    """Returns the name-musicbrainzid dictionary. If it's not stored, creates a brand new file"""
    filepath = '%s/artists.json' % common.get_output_path()
    if os.path.isfile(filepath):
        return json.load(open(filepath))
    else:
        artists = {}
        with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist' % common.get_output_path()) as tsvfile:
            artists = get_musicbrainz_artists_from_dump(tsvfile, 2, 0, 20)
            artists.update(get_musicbrainz_artists_from_dump(tsvfile, 3, 0, 20))

        with open('%s/musicbrainz_dump_20180725-001823/mbdump/artist_alias' % common.get_output_path()) as tsvfile:
            artists.update(get_musicbrainz_artists_from_dump(tsvfile, 2, 1, 16))
            artists.update(get_musicbrainz_artists_from_dump(tsvfile, 7, 1, 16))

        json.dump(artists, open(filepath, 'w'), indent=2, ensure_ascii=False)
        return artists

@click.command()
def equal_strings_match():
    """Creates the equal strings match output file"""
    # Wikidata sample loading
    labels_qid = json.load(open('musicians_wikidata_sample.json'))
    matches = matching_strategies.equal_strings_match((labels_qid, get_label_musicbrainzid_dict()))
    json.dump(matches, open('%s/equal_strings_matches.json' % common.get_output_path(), 'w'), indent=2, ensure_ascii=False)

def get_url_domains():
    """Finds all the domains to which the artists are connected"""
    fieldnames = [i for i in range(0, 5)]
    domain_regex = '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)'
    domains = defaultdict(int)
    with open('%s/musicbrainz_dump_20180725-001823/mbdump/url' % common.get_output_path(), "r") as tsvfile:
        urls = csv.DictReader(tsvfile, dialect='excel-tab', fieldnames=fieldnames)
        for url in urls:
            domain = re.search(domain_regex, url[2]).group(1)
            domains[domain] += 1
    towrite = {domain:count for (domain, count) in domains.items() if count > 10000}
    json.dump(towrite, open('%s/urls.json' % common.get_output_path(), 'w'), indent=2, ensure_ascii=False)

@click.command()
@click.argument('dump_folder_path', type=click.Path(exists=True))
@click.option('--output', '-o', default=common.get_output_path(), type=click.Path(exists=True))
def get_users_urls(dump_folder_path, output):
    """Creates the json containing url - artist id"""
    output_full_path = os.path.join(output, 'url_artist.json')
    urlid_id = defaultdict(str)
    url_id = defaultdict(str)

    if os.path.isfile(output_full_path):
        return json.load(open(output_full_path))
    else:
        with open(os.path.join(dump_folder_path, 'mbdump/l_artist_url'), "r") as tsvfile:
            fieldnames = [i for i in range(0, 6)]
            url_relationships = csv.DictReader(tsvfile, dialect='excel-tab', fieldnames=fieldnames)
            for relationship in url_relationships:
                # url id matched with its user id
                urlid_id[relationship[3]] = relationship[2]

        with open(os.path.join(dump_folder_path, 'mbdump/url'), "r") as tsvfile:
            fieldnames = [i for i in range(0, 5)]
            urls = csv.DictReader(tsvfile, dialect='excel-tab', fieldnames=fieldnames)
            for url in urls:
                # Translates the url ids stored before in the respective urls
                if url[0] in urlid_id:
                    url_id[url[2]] = urlid_id[url[0]]

        json.dump(url_id, open(output_full_path, 'w'), indent=2, ensure_ascii=False)
        return url_id

@click.command()
@click.argument('dump_folder_path', type=click.Path(exists=True))
@click.argument('links_qid_dictionary', type=click.Path(exists=True))
@click.argument('sitelinks_qid_dictionary', type=click.Path(exists=True))
@click.option('--output', '-o', default=common.get_output_path(), type=click.Path(exists=True))
def links_match(dump_folder_path, links_qid_dictionary, sitelinks_qid_dictionary, output):
    # Loads url-musicbrainz id 
    url_mbid = get_users_urls(dump_folder_path, output)
    # Loads link - wikidata id
    link_qid = json.load(open(links_qid_dictionary))
    # Loads sitelink - wikidata id
    sitelink_qid = json.load(open(sitelinks_qid_dictionary))
    # Equal strigs match among urls
    wikidata_full = {}.update(link_qid)
    wikidata_full.update(sitelink_qid)

    ids_matching = matching_strategies.equal_strings_match(url_mbid, wikidata_full)

    full_outputh_path = os.path.join(output, 'link_match.json')
    print(full_outputh_path)
    json.dump(ids_matching, open(full_outputh_path, 'w'), indent=2, ensure_ascii=False)
