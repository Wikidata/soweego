#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Goal: create a sourced identifier statement as in the following example.
Claim = (Joey Ramone, MusicBrainz artist ID, 2f3f8fb1-e5dc-4548-9601-fada0485e561)
Reference = [ (stated in, MusicBrainz), (retrieved, TIMESTAMP) ]
"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
from datetime import date

import pywikibot

LOGGER = logging.getLogger(__name__)

SITE = pywikibot.Site("wikidata", "wikidata")
REPO = SITE.data_repository()

BIBSYS_ID = 'P1015'
DISCOGS_ARTIST_ID = 'P1953'
MUSICBRAINZ_ARTIST_ID = 'P434'
TWITTER_USERNAME = 'P2002'

BIBSYS = pywikibot.ItemPage(REPO, 'Q4584301')
DISCOGS = pywikibot.ItemPage(REPO, 'Q504063')
MUSICBRAINZ = pywikibot.ItemPage(REPO, 'Q14005')
TWITTER = pywikibot.ItemPage(REPO, 'Q918')

# (stated in, CATALOG) reference
STATED_IN_PID = 'P248'
STATED_IN_REFERENCE = pywikibot.Claim(REPO, STATED_IN_PID, is_reference=True)
STATED_IN_REFERENCE.setTarget(MUSICBRAINZ)

# (retrieved, TIMESTAMP) reference
RETRIEVED_PID = 'P813'
TODAY = date.today()
TIMESTAMP = pywikibot.WbTime(
    site=REPO, year=TODAY.year, month=TODAY.month, day=TODAY.day, precision='day')
RETRIEVED_REFERENCE = pywikibot.Claim(REPO, RETRIEVED_PID, is_reference=True)
RETRIEVED_REFERENCE.setTarget(TIMESTAMP)

# Run on sandbox item
sandbox = pywikibot.ItemPage(REPO, 'Q4115189')
claim = pywikibot.Claim(REPO, 'P434')
claim.setTarget('2f3f8fb1-e5dc-4548-9601-fada0485e561')
sandbox.addClaim(claim)
claim.addSources([STATED_IN_REFERENCE, RETRIEVED_REFERENCE])
