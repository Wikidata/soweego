#!/usr/bin/env python
# coding: utf-8

import json
from collections import OrderedDict

import requests

# All mix'n'match catalogs
mnm = requests.get('https://tools.wmflabs.org/mix-n-match/overview.json').json()
mnm = mnm['data']

# Mix'n'match catalogs about people (class Q5, 'human')
# Keys explanation:
# total_entries = # catalog entries
# in_wikidata = curated / total entries ratio
# unable_to_match = entries that could not be matched by mix'n'match / total entries ratio
# matched_to_be_curated = non-curated / total entries ratio
people = {
    mnm[db]['name']: {
        'total_entries': int(mnm[db]['total']),
        'in_wikidata': float(int(mnm[db]['manual']) / int(mnm[db]['total'])),
        'unable_to_match': float(int(mnm[db]['noq']) / int(mnm[db]['total'])),
        'matched_to_be_curated': float(int(mnm[db]['autoq']) / int(mnm[db]['total'])),
        'url': mnm[db]['url'],
    }
    for db in mnm.keys()
    if mnm[db]['types'] == 'Q5'
}
# Extra dicts sorted by useful values in descending order
people_by_total_entries = OrderedDict(
    sorted(people.items(), key=lambda x: x[1]['total_entries'], reverse=True)
)
people_by_unmatched_entries = OrderedDict(
    sorted(people.items(), key=lambda x: x[1]['unable_to_match'], reverse=True)
)
people_by_entries_in_wd = OrderedDict(
    sorted(people.items(), key=lambda x: x[1]['in_wikidata'], reverse=True)
)
json.dump(
    people_by_total_entries,
    open('mnm_people_catalogs_by_total_entries.json', 'w'),
    indent=2,
    ensure_ascii=False,
)
json.dump(
    people_by_unmatched_entries,
    open('mnm_people_catalogs_by_unmatched_entries.json', 'w'),
    indent=2,
    ensure_ascii=False,
)
json.dump(
    people_by_entries_in_wd,
    open('mnm_people_catalogs_by_entries_in_wikidata.json', 'w'),
    indent=2,
    ensure_ascii=False,
)

# All SQID Wikidata properties
sqid = requests.get('https://tools.wmflabs.org/sqid/data/properties.json').json()
# SQID properties having external IDs as values
sqid_all = {
    pid: {
        'sqid_total_wikidata_statements': sqid[pid]['s'],
        'sqid_name': sqid[pid]['l'],
        'formatter_url': sqid[pid].get('u'),
        'sqid_pid': pid,
    }
    for pid in sqid.keys()
    if sqid[pid].get('d') == 'ExternalId'
}

# Mix'n'match catalogs about people with property ID
mnm_people_with_pid = {
    mnm[db]['wd_prop']: {
        'mnm_total_db_entries': int(mnm[db]['total']),
        'mnm_in_wikidata': float(int(mnm[db]['manual']) / int(mnm[db]['total'])),
        'mnm_unable_to_match': float(int(mnm[db]['noq']) / int(mnm[db]['total'])),
        'mnm_matched_to_be_curated': float(
            int(mnm[db]['autoq']) / int(mnm[db]['total'])
        ),
        'mnm_name': mnm[db]['name'],
        'mnm_pid': mnm[db]['wd_prop'],
        'mnm_url': mnm[db]['url'],
    }
    for db in mnm.keys()
    if mnm[db]['wd_prop'] and mnm[db]['types'] == 'Q5'
}
mnm_and_sqid = mnm_people_with_pid.copy()
# Merge SQID data into mix'n'match catalogs
for pid in mnm_and_sqid.keys():
    mnm_and_sqid[pid].update(sqid_all.get(pid, {}))
# Rebuild a readable dict with catalog names as keys
final = {v['mnm_name']: v for db, v in mnm_and_sqid.items()}
# Extra dicts sorted by useful values in descending order
by_sqid_usage = OrderedDict(
    sorted(
        final.items(),
        key=lambda x: x[1].get('sqid_total_wikidata_statements', 0),
        reverse=True,
    )
)
by_mnm_entries = OrderedDict(
    sorted(final.items(), key=lambda x: x[1]['mnm_total_db_entries'], reverse=True)
)
json.dump(
    by_sqid_usage,
    open('sqid_and_mnm_by_sqid_usage.json', 'w'),
    indent=2,
    ensure_ascii=False,
)
# This is the final one that should be used
json.dump(
    by_mnm_entries,
    open('sqid_and_mnm_by_mnm_entries.json', 'w'),
    indent=2,
    ensure_ascii=False,
)
