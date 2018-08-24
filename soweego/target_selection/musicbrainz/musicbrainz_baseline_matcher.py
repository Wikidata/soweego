import csv
import json
import os
import re
from collections import defaultdict
from urllib.parse import urlparse

import click
from soweego.target_selection.common import matching_strategies


# Utilities functions
def get_dates_strings_combinations(date_items):
    if len(date_items) > 0:
        date_items = [d if d != '\\N' else '' for d in date_items]
        combinations = []
        birth_strings = [str(date_items[0]),
                         '%s%s' % (date_items[0], date_items[1]),
                         '%s%s%s' % (date_items[0], date_items[1], date_items[2])]
        death_strings = [str(date_items[3]),
                         '%s%s' % (date_items[3], date_items[4]),
                         '%s%s%s' % (date_items[3], date_items[4], date_items[5])]

        for b in birth_strings:
            for d in death_strings:
                combinations.append('%s-%s' % (b, d))

        return list(set(combinations))
    else:
        return []


def check_row_dump_describes_person(row):
    return row[10] == '1' or row[10] == '\\N'


def get_musicbrainz_artists_from_dump(opened_file_dump,
                                      label_column_index,
                                      id_column_index,
                                      total_number_columns):
    """Given an opened musicbrainz(mb) dump, return a dictionary label - mbid"""

    label_musicbrainz = {}
    fieldnames = [i for i in range(0, total_number_columns)]

    musicbrainz_artists = csv.DictReader(opened_file_dump,
                                         dialect='excel-tab',
                                         fieldnames=fieldnames)

    for row in musicbrainz_artists:
        # Checks if it's a person
        if check_row_dump_describes_person(row):
            lowered_label = row[label_column_index].lower()
            label_musicbrainz[lowered_label] = row[id_column_index]

    return label_musicbrainz


@click.command()
@click.argument('musicbrainz_dump_folder', type=click.Path(exists=True))
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def get_label_musicbrainzid_dict(musicbrainz_dump_folder, output):
    """Creates the JSON name-musicbrainzid."""

    artist_table_path = os.path.join(
        musicbrainz_dump_folder, 'mbdump', 'artist')
    artist_alias_table_path = os.path.join(
        musicbrainz_dump_folder, 'mbdump', 'artist_alias')

    filepath = os.path.join(output, 'artists.json')

    artists = {}
    with open(artist_table_path) as tsvfile:
        artists = get_musicbrainz_artists_from_dump(tsvfile, 2, 0, 20)
        artists.update(
            get_musicbrainz_artists_from_dump(tsvfile, 3, 0, 20))

    with open(artist_alias_table_path) as tsvfile:
        artists.update(
            get_musicbrainz_artists_from_dump(tsvfile, 2, 1, 16))
        artists.update(
            get_musicbrainz_artists_from_dump(tsvfile, 7, 1, 16))

    json.dump(artists, open(filepath, 'w'), indent=2, ensure_ascii=False)


@click.command()
@click.argument('musicbrainz_dump_folder', type=click.Path(exists=True))
@click.option('--threshold', '-t', default=10000, type=int)
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def get_url_domains(musicbrainz_dump_folder, threshold, output):
    """Finds all the domains to which the artists are connected"""

    fieldnames = [i for i in range(0, 5)]
    domains = defaultdict(int)
    url_table_path = os.path.join(musicbrainz_dump_folder, 'mbdump', 'url')

    with open(url_table_path, "r") as tsvfile:
        urls = csv.DictReader(
            tsvfile, dialect='excel-tab', fieldnames=fieldnames)
        for url in urls:
            domain = urlparse(url[2]).netloc
            domains[domain] += 1
    towrite = {domain: count for (domain, count)
               in domains.items() if count > threshold}

    output_path = os.path.join(output, 'urls.json')
    json.dump(towrite, open(output_path, 'w'), indent=2, ensure_ascii=False)


def _get_users_urls(dump_folder_path, output):
    output_full_path = os.path.join(output, 'url_artist.json')
    urlid_id = defaultdict(str)
    url_id = defaultdict(str)

    if os.path.isfile(output_full_path):
        return json.load(open(output_full_path))
    else:
        with open(os.path.join(dump_folder_path, 'mbdump/l_artist_url'), "r") as tsvfile:
            fieldnames = [i for i in range(0, 6)]
            url_relationships = csv.DictReader(
                tsvfile, dialect='excel-tab', fieldnames=fieldnames)
            for relationship in url_relationships:
                # url id matched with its user id
                urlid_id[relationship[3]] = relationship[2]

        with open(os.path.join(dump_folder_path, 'mbdump/url'), "r") as tsvfile:
            fieldnames = [i for i in range(0, 5)]
            urls = csv.DictReader(
                tsvfile, dialect='excel-tab', fieldnames=fieldnames)
            for url in urls:
                # Translates the url ids stored before in the respective urls
                if url[0] in urlid_id:
                    url_id[url[2]] = urlid_id[url[0]]

        json.dump(url_id, open(output_full_path, 'w'),
                  indent=2, ensure_ascii=False)
        return url_id


@click.command()
@click.argument('dump_folder_path', type=click.Path(exists=True))
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def get_users_urls(dump_folder_path, output):
    """Creates the json containing url - artist id"""
    _get_users_urls(dump_folder_path, output)


@click.command()
@click.argument('dump_folder_path', type=click.Path(exists=True))
@click.argument('links_qid_dictionary', type=click.Path(exists=True))
@click.argument('sitelinks_qid_dictionary', type=click.Path(exists=True))
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def links_match(dump_folder_path, links_qid_dictionary, sitelinks_qid_dictionary, output):
    # Loads url-musicbrainz id
    url_mbid = _get_users_urls(dump_folder_path, output)
    # Loads link - wikidata id
    link_qid = json.load(open(links_qid_dictionary))
    # Loads sitelink - wikidata id
    sitelink_qid = json.load(open(sitelinks_qid_dictionary))
    # Equal strigs match among urls
    link_qid.update(sitelink_qid)

    ids_matching = matching_strategies.perfect_string_match(
        (url_mbid, link_qid))

    full_outputh_path = os.path.join(output, 'link_match.json')
    json.dump(ids_matching, open(full_outputh_path, 'w'),
              indent=2, ensure_ascii=False)


@click.command()
@click.argument('dump_folder_path', type=click.Path(exists=True))
@click.argument('label_mbid_dict', type=click.Path(exists=True))
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def get_users_label_dates_dictionary(dump_folder_path, label_mbid_dict, output):

    mbid_dateelements = defaultdict(list)
    labeldate_mbid = {}

    artist_table_path = os.path.join(
        dump_folder_path, 'mbdump', 'artist')

    with open(artist_table_path, mode='r') as tsvfile:
        fieldnames = [i for i in range(0, 20)]

        musicbrainz_artists = csv.DictReader(tsvfile,
                                             dialect='excel-tab',
                                             fieldnames=fieldnames)
        # Dates extraction from the dump
        for row in musicbrainz_artists:
            if check_row_dump_describes_person(row):
                date_items = [
                    row[4], row[5], row[6], row[7], row[8], row[9]]
                # Avoids to add people with no dates in the dictionary
                if len(set(date_items)) > 1:
                    mbid_dateelements[row[0]] = date_items

    label_mbid = json.load(open(label_mbid_dict, 'r'))

    for label, mbid in label_mbid.items():
        # gets the dates for each mbid with label
        for c in get_dates_strings_combinations(mbid_dateelements[mbid]):
            labeldate_mbid['%s|%s' % (label, c)] = mbid

    full_outputh_path = os.path.join(output, 'labeldates_mbid.json')
    json.dump(labeldate_mbid, open(full_outputh_path, 'w'),
              indent=2, ensure_ascii=False)
