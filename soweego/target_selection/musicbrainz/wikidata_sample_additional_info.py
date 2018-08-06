import json
from common import *
import re

path = get_output_path()

def query_info_for(qids_bucket):
    #TODO
    #creating the big query with the optional chain
    properties = json.load(open('resources/properties_mapping.json'))
    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    for k, v in properties.items():
        query += 'OPTIONAL { ?id wdt:%s ?%s . } ' % (v, re.sub('\W', '', k))
    query += "}"
    print(query)
    # do it foreach bucket
    #store everything in a file

def query_wikipedia_articles_for(qids_bucket):
    properties = json.load(open('resources/properties_mapping.json'))
    query = "SELECT * WHERE{ VALUES ?id { %s } " % ' '.join(qids_bucket)
    query += 'OPTIONAL { ?article schema:about ?id . }'
    query += "}"
    print(query)


#TODO
#buckets of a hundred ids
#call the function

labels_qid = json.load(open('../../wikidata/resources/musicians_sample_labels.json'))
entities = ["wd:%s"%v for k,v in labels_qid.items()]
bucket_size = 100
buckets = [set(entities[i*bucket_size:(i+1)*bucket_size]) for i in range(0, int((len(entities)/bucket_size+1)))]
query_wikipedia_articles_for(buckets[0])