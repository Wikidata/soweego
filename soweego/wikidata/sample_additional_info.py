import json
import csv
import os
import re
import iso8601
from collections import defaultdict
import click
from ..target_selection.musicbrainz import common

# Queries computing

def query_info_for(qids_bucket, properties):
    """Given a list of wikidata entities returns a query for getting some external ids"""

    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    for k, v in properties.items():
        query += 'OPTIONAL { ?id wdt:%s %s . } ' % (v, k)
    query += "}"
    return query

def query_wikipedia_articles_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting wikidata articles"""

    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    query += 'OPTIONAL { ?article schema:about ?id . }'
    query += "}"
    return query

def query_birth_death(qids_bucket):
    """Given a list of wikidata entities returns a query for getting their birth and death dates"""

    query = "SELECT ?id ?birth ?b_precision ?death ?d_precision WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    query += '?id p:P569 ?b. ?b psv:P569 ?t1 . ?t1 wikibase:timePrecision ?b_precision . ?t1 wikibase:timeValue ?birth . OPTIONAL { ?id p:P570 ?d . ?d psv:P570 ?t2 . ?t2 wikibase:timePrecision ?d_precision . ?t2 wikibase:timeValue ?death . }'
    query += "}"

    return query

#JSONs creation
#TODO convert to command using click

def get_url_formatters_for_properties(properties):
    """Retrieves the url formatters for the properties listed in the given dict"""
    filepath = os.path.join(common.get_output_path(), 'url_formatters.json')

    if os.path.isfile(filepath):
        return json.load(open(filepath))
    else:
        formatters = {}
        for prop_name, prop_id in properties.items():
            query = "SELECT * WHERE { %s wdt:P1630 ?formatterUrl . }" % ('wd:%s' % prop_id)
            reader = csv.DictReader(common.api_request_wikidata(query), dialect='excel-tab')
            for r in reader:
                formatters[prop_name] = r['?formatterUrl']

        json.dump(formatters, open(filepath, 'w'), indent=2, ensure_ascii=False)
        return formatters

# Utils
def get_wikidata_id_from_uri(uri):
    '''Given a wikidata entity uri, returns only the id'''
    return re.search(r'\/(\w+)>', uri).group(1)

def stripe_first_last_characters(string):
    '''Given a string removes the first and the last characters'''
    return string[1:-1]

def get_sample_buckets(sample_path):
    '''Given a sample path, returns it divided in equal size buckets'''
    size = 100
    labels_qid = json.load(open(sample_path))
    entities = ["wd:%s"%v for k,v in labels_qid.items()]
    return [set(entities[i*size:(i+1)*size]) for i in range(0, int((len(entities)/size+1)))]

def get_date_strings(timestamp, precision):
    """Given a timestamp and a wikidata date precision, returns a combination of strings"""
    if timestamp:
        timesplit = timestamp.split('^')[0]
        precisionsplit = precision.split('^')[0]
        date = iso8601.parse_date(timesplit).date()

        if precisionsplit == "11":
            return [str(date.year), '%s%s' % (date.year, date.month), '%s%s%s' % (date.year, date.month, date.day)]
        elif precisionsplit == "10":
            return [str(date.year), '%s%s' % (date.year, date.month)]
        else:
            return [str(date.year)]
    else:
        return []

@click.command()
@click.argument('sample_path', type=click.Path(exists=True))
@click.argument('property_mapping_path', type=click.Path(exists=True))
@click.option('--output', '-o', default=common.get_output_path(), type=click.Path(exists=True))
def get_links_for_sample(sample_path, property_mapping_path, output):
    '''Creates the JSON containing url - wikidata id'''

    properties = {'?%s' % re.sub(r'\W', '', k): v  for k, v in json.load(open(property_mapping_path)).items()}

    formatters_dict = get_url_formatters_for_properties(properties)

    filepath = os.path.join(output, 'sample_links.json')

    # Creates buckets for artist from the sample. Technique to fix quering issues
    buckets = get_sample_buckets(sample_path)

    url_id = defaultdict(str)

    for bucket in buckets:
        # Downloads the first bucket
        response = common.api_request_wikidata(query_info_for(bucket, properties))
        ids_collection = csv.DictReader(response, dialect='excel-tab')
        for id_row in ids_collection:
            # Extracts the wikidata id from the URI
            entity_id = get_wikidata_id_from_uri(id_row['?id'])
            url_id[stripe_first_last_characters(id_row['?id'])] = entity_id
            # Foreach id in the response, creates the full url and adds it to the dict
            for col in ids_collection.fieldnames:
                if col != '?id' and id_row[col]:
                    if formatters_dict.get(col):
                        url_id[formatters_dict[col].replace('$1', id_row[col])] = entity_id
                    else:
                        print('%s does not have an entry in the formatters file' % col)

    json.dump(url_id, open(filepath, 'w'), indent=2, ensure_ascii=False)

@click.command()
@click.argument('sample_path', type=click.Path(exists=True))
@click.option('--output', '-o', default=common.get_output_path(), type=click.Path(exists=True))
def get_sitelinks_for_sample(sample_path, output):
    '''Given a sample of users, retrieves all the sitelinks'''

    filepath = os.path.join(output, 'sample_sitelinks.json')

    # Creates buckets for artist from the sample. Technique to fix quering issues
    buckets = get_sample_buckets(sample_path)

    url_id = defaultdict(str)

    for bucket in buckets:
        # Downloads the first bucket
        response = common.api_request_wikidata(query_wikipedia_articles_for(bucket))
        articles_collection = csv.DictReader(response, dialect='excel-tab')
        for article_row in articles_collection:
            site_url = stripe_first_last_characters(article_row['?article'])
            entity_id = get_wikidata_id_from_uri(article_row['?id'])
            url_id[site_url] = entity_id

    json.dump(url_id, open(filepath, 'w'), indent=2, ensure_ascii=False)

@click.command()
@click.argument('sample_path', type=click.Path(exists=True))
@click.option('--output', '-o', default='output', type=click.Path(exists=True))
def get_birth_death_dates_for_sample(sample_path, output):
    # Creates buckets for artist from the sample. Technique to fix quering issues
    qid_labels = {v: k for k, v in json.load(open(sample_path, 'r')).items()}
    buckets = get_sample_buckets(sample_path)

    labeldate_qid = {}
    filepath = os.path.join(output, 'sample_dates.json')

    for bucket in buckets:
        response = common.api_request_wikidata(query_birth_death(bucket))
        dates_collection = csv.DictReader(response, dialect='excel-tab')
        for date_row in dates_collection:
            qid = get_wikidata_id_from_uri(date_row['?id'])
            # creates the combination of all birth dates strings and all death dates strings
            if date_row['?birth']:
                for b in get_date_strings(date_row['?birth'], date_row['?b_precision']):
                    if date_row['?death']:
                        for d in get_date_strings(date_row['?death'], date_row['?d_precision']):
                            labeldate_qid['%s|%s-%s' % (qid_labels[qid], b, d)] = qid
                    else:
                        labeldate_qid['%s|%s' % (qid_labels[qid], b)] = qid
            else:
                for d in get_date_strings(date_row['?death'], date_row['?d_precision']):
                    labeldate_qid['%s|-%s' % (qid_labels[qid], d)] = qid

    json.dump(labeldate_qid, open(filepath, 'w'), indent=2, ensure_ascii=False)
