import os
import requests

def get_path():
    """Return the path of the current module"""
    path = os.path.abspath(__file__)
    return os.path.dirname(path)

def get_output_path():
    """Returns the path of the output files"""
    output_dir_path = '%s/output' % get_path()

    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)

    return output_dir_path

def api_request_wikidata(query):
    '''Given a wikidata sparql query returns the lines of the tsv response'''
    r = requests.get('https://query.wikidata.org/sparql', params={'query': query}, headers={'Accept': 'text/tab-separated-values'})
    return r.text.splitlines()