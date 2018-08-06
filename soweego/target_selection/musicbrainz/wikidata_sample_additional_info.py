import json
import sys #debug
import click
import csv
import os
import re
from . import common
from collections import defaultdict

PATH = common.get_output_path()
PROPERTIES_MAPPING_PATH = '%s/resources/properties_mapping.json' % common.get_path()
PROPERTIES = {'?%s' % re.sub(r'\W', '', k): v  for k, v in json.load(open(PROPERTIES_MAPPING_PATH)).items()}


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
    filepath = '%s/url_formatters.json' % common.get_output_path()

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

@click.command()
def get_wikidata_sample_links():
    '''Creates the JSON containing url - wikidata id'''

    formatters_dict = get_url_formatters_for_properties()

    # TODO parametro per sceglire sample
    filepath = '%s/wikidata_musician_sample_links.json' % common.get_output_path()

    # Creates buckets for artist from the sample. Technique to fix quering issues
    labels_qid = json.load(open('soweego/wikidata/resources/musicians_sample_labels.json'))
    entities = ["wd:%s"%v for k,v in labels_qid.items()]
    BUCKET_SIZE = 100
    buckets = [set(entities[i*BUCKET_SIZE:(i+1)*BUCKET_SIZE]) for i in range(0, int((len(entities)/BUCKET_SIZE+1)))]

    url_id = defaultdict(str)

    for bucket in buckets:
        # Downloads the first bucket
        ids_collection = csv.DictReader(common.api_request_wikidata(query_info_for(bucket)), dialect='excel-tab')
        for id_row in ids_collection:
            # Extracts the wikidata id from the URI
            entity_id = re.search(r'\/(\w+)>', id_row['?id']).group(1)
            # Foreach id in the response, creates the full url and adds it to the dict
            for col in ids_collection.fieldnames:
                if col != '?id' and id_row[col]:
                    url_id[formatters_dict[col].replace('$1', id_row[col])] = entity_id

    json.dump(url_id, open(filepath, 'w'), indent=2, ensure_ascii=False)

@click.command()
def get_sitelinks_for_sample():
    '''Given a sample of users, retrieves all the sitelinks'''

    #TODO sample as parameter    
    filepath = '%s/wikidata_musician_sample_sitelinks.json' % common.get_output_path()

    # TODO refactor buckets in a function
    # Creates buckets for artist from the sample. Technique to fix quering issues
    labels_qid = json.load(open('soweego/wikidata/resources/musicians_sample_labels.json'))
    entities = ["wd:%s"%v for k,v in labels_qid.items()]
    BUCKET_SIZE = 100
    buckets = [set(entities[i*BUCKET_SIZE:(i+1)*BUCKET_SIZE]) for i in range(0, int((len(entities)/BUCKET_SIZE+1)))]

    url_id = defaultdict(str)

    for bucket in buckets:
        # Downloads the first bucket
        articles_collection = csv.DictReader(common.api_request_wikidata(query_wikipedia_articles_for(bucket)), dialect='excel-tab')
        for article_row in articles_collection:
            #TODO id extraction from wikidata uri asa a function
            site_url = article_row['?article'][1:-1]
            url_id[site_url] = re.search(r'\/(\w+)>', article_row['?id']).group(1)
    
    json.dump(url_id, open(filepath, 'w'), indent=2, ensure_ascii=False)