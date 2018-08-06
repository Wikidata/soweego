import json
import click
import csv
import os
import re
from . import common

PATH = common.get_output_path()
PROPERTIES_MAPPING_PATH = '%s/resources/properties_mapping.json' % common.get_path()
properties = {'?%s' % re.sub(r'\W', '', k): v  for k, v in json.load(open(PROPERTIES_MAPPING_PATH)).items()}

def query_info_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting some external ids"""

    properties = json.load(open(PROPERTIES_MAPPING_PATH))
    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    for k, v in properties.items():
        query += 'OPTIONAL { ?id wdt:%s ?%s . } ' % (v, k)
    query += "}"
    return query

def query_wikipedia_articles_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting wikidata articles"""

    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    query += 'OPTIONAL { ?article schema:about ?id . }'
    query += "}"
    return query

def get_url_formatters_for_properties():
    """Retrieves the url formatters for the properties listed in properties_mapping.json"""
    filepath = '%s/url_formatters.json' % common.get_output_path()

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

@click.command()
def get_wikidata_sample_links():
    labels_qid = json.load(open('soweego/wikidata/resources/musicians_sample_labels.json'))
    entities = ["wd:%s"%v for k,v in labels_qid.items()]
    BUCKET_SIZE = 100
    buckets = [set(entities[i*BUCKET_SIZE:(i+1)*BUCKET_SIZE]) for i in range(0, int((len(entities)/BUCKET_SIZE+1)))]

    print(get_url_formatters_for_properties())


#TODO Code for api request
#TODO url formatting