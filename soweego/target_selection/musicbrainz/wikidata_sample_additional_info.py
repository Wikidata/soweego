import json
import re
from common import get_output_path

PATH = get_output_path()

def query_info_for(qids_bucket):
    """Given a list of wikidata entities returns a query for getting some external ids"""

    properties = json.load(open('resources/properties_mapping.json'))
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

labels_qid = json.load(open('../../wikidata/resources/musicians_sample_labels.json'))
entities = ["wd:%s"%v for k,v in labels_qid.items()]
BUCKET_SIZE = 100
buckets = [set(entities[i*BUCKET_SIZE:(i+1)*BUCKET_SIZE]) for i in range(0, int((len(entities)/BUCKET_SIZE+1)))]

for bucket in buckets:
    print(query_wikipedia_articles_for(bucket))
