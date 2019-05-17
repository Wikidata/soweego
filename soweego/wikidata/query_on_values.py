#!/usr/bin/env python3
# coding: utf-8

from sys import argv, exit

import click
import requests


@click.command()
def main(items_path, sparql_condition, output_path):
    """Run a SPARQL query against the Wikidata endpoint using batches of 500 items"""
    entities = ['wd:%s' % l.rstrip() for l in open(items_path).readlines()]
    buckets = [entities[i*500:(i+1)*500] for i in range(0, int((len(entities)/500+1)))]
    with open(output_path, 'w', 1) as o:
        for b in buckets:
            query = 'select ?person where { values ?person { %s } . %s }' % (' '.join(b), sparql_condition)
            r = requests.get('https://query.wikidata.org/sparql', params={'query': query}, headers={'Accept': 'text/tab-separated-values'})
            o.write(r.text.replace('?person\r\n', ''))
            print('Request OK?', r.ok)
    return 0


if __name__ == '__main__':
    if len(argv) != 4:
        print('Usage: python %s ITEMS_PATH SPARQL_CONSTRAINT OUTPUT_PATH' % __file__)
        exit(1)
    exit(main(argv[1], argv[2], argv[3]))
