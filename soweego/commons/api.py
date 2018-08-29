import requests


def api_request_wikidata(query):
    '''Given a wikidata sparql query returns the lines of the tsv response'''
    r = requests.get('https://query.wikidata.org/sparql',
                     params={'query': query}, headers={'Accept': 'text/tab-separated-values'})
    return r.text.splitlines()
