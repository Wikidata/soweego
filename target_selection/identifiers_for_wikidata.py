# coding: utf-8
import requests
entities = [l.rstrip() for l in open('1_percent_sample').readlines()]
buckets = [entities[i*100:(i+1)*100] for i in range(0, int((len(entities)/100+1)))]
with open('linked_1_percent_sample.tsv', 'w') as o:
    for b in buckets:
        query = 'SELECT * WHERE { VALUES ?person { %s } optional { ?person wdt:P214 ?viaf } optional { ?person wdt:P213 ?isni } optional { ?person wdt:P227 ?gnd } optional { ?person wdt:P244 ?loc } optional { ?person wdt:P268 ?bnf } }' % ' '.join(b)
        r = requests.get('https://query.wikidata.org/sparql', params={'query': query}, headers={'Accept': 'text/tab-separated-values'})
        o.write(r.text)
        print('OK?', r.ok)
        
