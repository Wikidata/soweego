import json
from common import *
import re

path = get_output_path()

labels_qid = json.load(open('../../wikidata/resources/musicians_sample_labels.json'))

def query_info_for(qids_bucket):
    #TODO
    #creating the big query with the optional chain
    properties = json.load(open('resources/properties_mapping.json'))
    optionals = ""
    for k, v in properties.items():
        optionals += 'OPTIONAL { ?id wdt:%s ?%s . }\n' % (v, re.sub('\W', '', k))
    print(optionals)
    # do it foreach bucket
    #store everything in a file


#TODO
#buckets of a hundred ids
#call the function
query_info_for(0)