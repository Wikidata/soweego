# coding: utf-8
import json
import csv
import gzip
dates = {}
with gzip.open('name.basics.tsv.gz', 'rt') as i:
    no_nascita = 0
    no_morte = 0
    nessuna_data = 0
    ds = csv.DictReader(i, delimiter='\t')
    for l in ds:
        ide = l['nconst']
        nome = l['primaryName'].lower()
        nascita = l['birthYear']
        morte = l['deathYear']
        if nascita != r'\N' and morte != r'\N':
            nessuna_data += 1
            continue
        sb = []
        sb.append(nome)
        sb.append('|')
        ce_nascita = False
        if nascita != r'\N':
            sb.append(nascita)
            ce_nascita = True
        else:
            no_nascita += 1
        if morte != r'\N':
            if ce_nascita:
                sb.append('-')
            sb.append(morte)
        else:
            no_morte += 1
        dates[''.join(sb)] = ide
        
json.dump(dates, open('dates_id.json', 'w'), indent=2, ensure_ascii=False)
