#!/usr/bin/env python
# coding: utf-8

import csv
import json

from collections import defaultdict

HOME = '/Users/focs/'

# Wikidata sample, labels
qid_labels = json.load(open(HOME + 'wikidata/final_1_percent_sample.json'))
label_qid = {}
for qid in qid_labels.keys():
    if qid_labels.get(qid):
        labels = qid_labels[qid].keys()
        for l in labels:
            label_qid[l.lower()] = qid
# Better sample with labels as keys
json.dump(label_qid, open(HOME + 'wikidata/label2qid_1_percent_sample.json', 'w'), indent=2, ensure_ascii=False)

# BNE, name labels
label_bne = {}
bne_names = csv.DictReader(open(HOME + 'bne/all_people_ids_and_names.csv'))
for row in bne_names:
    label_bne[row['name']].lower() = row['id'].replace('http://datos.bne.es/resource/', '')

# BNE, 'also known as' labels
aka_bne = {}
bne_aka = csv.DictReader(open(HOME + 'bne/aka_people'))
for row in bne_aka:
    aka_bne[row['aka'].lower()] = row['id'].replace('http://datos.bne.es/resource/', '')

### Baseline matcher 1: perfect strings
# Perfect matches against BNE names
matched = defaultdict(list)
for d in (label_qid, label_bne):
    for k,v in d.items():
        matched[k].append(v)
json.dump({v[0]: v[1] for v in matched.values() if len(v) > 1}, open('perfect_matches.json', 'w'), indent=2)

# Perfect matches against BNE AKA
matched = defaultdict(list)
for d in (label_qid, aka_bne):
    for k,v in d.items():
        matched[k].append(v)       
json.dump({v[0]: v[1] for v in matched.values() if len(v) > 1}, open('aka_perfect_matches.json', 'w'), indent=2)

# Links available in BNE
isni = 'http://isni-url.oclc.nl/isni/'
viaf = 'http://viaf.org/viaf/'
gnd = 'http://d-nb.info/gnd/'
loc = 'http://id.loc.gov/authorities/names/'
bnf = 'http://data.bnf.fr/'

# Wikidata sample, links
linked_wd = {}
wd_linked = csv.DictReader(open(HOME + 'wikidata/linked_1_percent_sample.tsv'), delimiter='\t')
for row in wd_linked:
    qid = row['?person'].replace('<http://www.wikidata.org/entity/', '').replace('>', '')
    if row.get('?viaf'):
        linked_wd[viaf + row['?viaf']] = qid
    if row.get('?isni'):
        linked_wd[isni + row['?isni'].replace(' ', '')] = qid
    if row.get('?gnd'):
        linked_wd[gnd + row['?gnd']] = qid
    if row.get('?loc'):
        linked_wd[loc + row['?loc']] = qid
    if row.get('?bnf'):
        linked_wd[bnf + row['?bnf']] = qid

# BNE, links
bne_linked = csv.DictReader(open(HOME + 'bne/linked_people'))
linked_bne = {}
for row in bne_linked:
    linked_bne[row['link']] = row['id'].replace('http://datos.bne.es/resource/', '')
 
### Baseline matcher 2: cross-catalogs links
matched = defaultdict(list)
for d in (linked_wd, linked_bne):
    for k,v in d.items():
        matched[k].append(v)       
json.dump({v[0]: v[1] for v in matched.values() if len(v) > 1}, open('link_matches.json', 'w'), indent=2)

### Baseline matcher 3: Wikipedia links
# BNE, DBpedia links
bbdb = filter(lambda x: 'dbpedia.org' in x, linked_bne)
dbp = {x.replace('http://dbpedia.org/resource/', ''): linked_bne[x] for x in bbdb}

# Wikidata sample, site links
site_qid = json.load(open(HOME + 'wikidata/site2qid_1_percent_sample.json'))
matched = defaultdict(list)
for d in (site_qid, dbp):
    for k,v in d.items():
        matched[k].append(v)        
json.dump({v[0]: v[1] for v in matched.values() if len(v) > 1}, open('enwiki__matches.json', 'w'), indent=2, ensure_ascii=False)

