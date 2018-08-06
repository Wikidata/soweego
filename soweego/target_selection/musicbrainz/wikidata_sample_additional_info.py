import json
import click
import re
from . import common

PATH = common.get_output_path()
PROPERTIES_MAPPING_PATH = '%s/resources/properties_mapping.json' % common.get_path()

def query_info_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting some external ids"""

    properties = json.load(open(PROPERTIES_MAPPING_PATH))
    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    for k, v in properties.items():
        query += 'OPTIONAL { ?id wdt:%s ?%s . } ' % (v, re.sub(r'\W', '', k))
    query += "}"
    return query
    # do it foreach bucket
    #store everything in a file

def query_wikipedia_articles_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting wikidata articles"""

    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    query += 'OPTIONAL { ?article schema:about ?id . }'
    query += "}"
    return query

def query_url_formatters_for_properties():
    """Retrieves the url formatters for the properties listed in properties_mapping.json"""

    properties = json.load(open(PROPERTIES_MAPPING_PATH))
    values = ['wd:%s'%v for _, v in properties.items()]
    query = "SELECT * WHERE { VALUES ?id {%s} . ?id wdt:P1630 ?formatterUrl . }" % ' '.join(values)
    return query

@click.command()
def get_wikidata_sample_links():
    labels_qid = json.load(open('soweego/wikidata/resources/musicians_sample_labels.json'))
    entities = ["wd:%s"%v for k,v in labels_qid.items()]
    BUCKET_SIZE = 100
    buckets = [set(entities[i*BUCKET_SIZE:(i+1)*BUCKET_SIZE]) for i in range(0, int((len(entities)/BUCKET_SIZE+1)))]

    common.api_request_wikidata(query_info_for(buckets[0]))


#TODO Code for api request
#TODO url formatting