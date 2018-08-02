#!/usr/bin/env python3
# coding: utf-8

import requests

WD = '/Users/focs/wikidata/'

entities = [l.rstrip() for l in open(WD + 'humans_1_percent_sample').readlines()]
buckets = [entities[i*100:(i+1)*100] for i in range(0, int((len(entities)/100+1)))]
with open(WD + 'dates_1_percent_sample.tsv', 'w') as o:
    for b in buckets:
        query = 'SELECT * WHERE { VALUES ?person { %s } . ?person wdt:P569 ?birth . optional { ?person wdt:P570 ?death } . }' % ' '.join(b)
        r = requests.get('https://query.wikidata.org/sparql', params={'query': query}, headers={'Accept': 'text/tab-separated-values'})
        o.write(r.text)
        print('OK?', r.ok)
        
