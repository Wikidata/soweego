#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of Wikidata vocabulary terms."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

# Sandbox items in production site
SANDBOX_1_QID = 'Q4115189'
SANDBOX_2_QID = 'Q13406268'
SANDBOX_3_QID = 'Q15397819'

# Properties used to get instances
INSTANCE_OF_PID = 'P31'
OCCUPATION_PID = 'P106'

# Properties used for references
STATED_IN_PID = 'P248'
RETRIEVED_PID = 'P813'

# Target catalog items
DISCOGS_QID = 'Q504063'
IMDB_QID = 'Q37312'
MUSICBRAINZ_QID = 'Q14005'
TWITTER_QID = 'Q918'

# Identifier properties
DISCOGS_ARTIST_PID = 'P1953'
IMDB_PID = 'P345'
MUSICBRAINZ_ARTIST_PID = 'P434'
TWITTER_USERNAME_PID = 'P2002'

# Target catalogs helper dictionary
CATALOG_MAPPING = {
    'discogs': {
        'qid': DISCOGS_QID,
        'pid': DISCOGS_ARTIST_PID
    },
    'imdb': {
        'qid': IMDB_QID,
        'pid': IMDB_PID
    },
    'musicbrainz': {
        'qid': MUSICBRAINZ_QID,
        'pid': MUSICBRAINZ_ARTIST_PID
    },
    'twitter': {
        'qid': TWITTER_QID,
        'pid': TWITTER_USERNAME_PID
    }
}

# Properties IDs with URL data type, from SPARQL query:
# SELECT ?property WHERE { ?property a wikibase:Property ; wikibase:propertyType wikibase:Url . }
URL_PIDS = set([
    'P854', 'P855', 'P856', 'P953', 'P963', 'P968', 'P973', 'P1019', 'P1065',
    'P1324', 'P1325', 'P1348', 'P1401', 'P1421', 'P1482', 'P1581', 'P1613',
    'P1628', 'P1709', 'P1713', 'P1896', 'P1957', 'P1991', 'P2035', 'P2078',
    'P2235', 'P2236', 'P2488', 'P2520', 'P2649', 'P2699', 'P2888', 'P3254',
    'P3268', 'P3950', 'P4001', 'P4238', 'P4570', 'P4656', 'P4765', 'P4945',
    'P4997', 'P5178', 'P5195', 'P5282', 'P5305', 'P5715'
])
