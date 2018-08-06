import json
import csv
import os
import re
from collections import defaultdict
import click
from . import common

PATH = common.get_output_path()
PROP_MAP_PATH = os.path.join(common.get_path(), 'resources/properties_mapping.json')
PROPERTIES = {'?%s' % re.sub(r'\W', '', k): v  for k, v in json.load(open(PROP_MAP_PATH)).items()}

# Queries computing

def query_info_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting some external ids"""

    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    for k, v in PROPERTIES.items():
        query += 'OPTIONAL { ?id wdt:%s %s . } ' % (v, k)
    query += "}"
    return query

def query_wikipedia_articles_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting wikidata articles"""

    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    query += 'OPTIONAL { ?article schema:about ?id . }'
    query += "}"
    return query

#JSONs creation

def get_url_formatters_for_properties():
    """Retrieves the url formatters for the properties listed in properties_mapping.json"""
    filepath = os.path.join(common.get_output_path(), 'url_formatters.json')

    if os.path.isfile(filepath):
        return json.load(open(filepath))
    else:
        formatters = {}
        for prop_name, prop_id in PROPERTIES.items():
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

@click.command()
def get_wikidata_sample_links():
    '''Creates the JSON containing url - wikidata id'''

    formatters_dict = get_url_formatters_for_properties()

    # TODO parametro per sceglire sample
    filepath = os.path.join(common.get_output_path(), 'wikidata_musician_sample_links.json')

    # Creates buckets for artist from the sample. Technique to fix quering issues
    buckets = get_sample_buckets('soweego/wikidata/resources/musicians_sample_labels.json')

    url_id = defaultdict(str)

    for bucket in buckets:
        # Downloads the first bucket
        response = common.api_request_wikidata(query_info_for(bucket))
        ids_collection = csv.DictReader(response, dialect='excel-tab')
        for id_row in ids_collection:
            # Extracts the wikidata id from the URI
            entity_id = get_wikidata_id_from_uri(id_row['?id'])
            url_id[stripe_first_last_characters(id_row['?id'])] = entity_id
            # Foreach id in the response, creates the full url and adds it to the dict
            for col in ids_collection.fieldnames:
                if col != '?id' and id_row[col]:
                    url_id[formatters_dict[col].replace('$1', id_row[col])] = entity_id

    json.dump(url_id, open(filepath, 'w'), indent=2, ensure_ascii=False)

@click.command()
def get_sitelinks_for_sample():
    '''Given a sample of users, retrieves all the sitelinks'''

    #TODO sample as parameter
    filepath = os.path.join([common.get_output_path(), 'wikidata_musician_sample_sitelinks.json'])

    # Creates buckets for artist from the sample. Technique to fix quering issues
    buckets = get_sample_buckets('soweego/wikidata/resources/musicians_sample_labels.json')

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
