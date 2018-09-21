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
