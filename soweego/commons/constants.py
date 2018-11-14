#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Constants"""

from soweego.importer.models import discogs_entity, musicbrainz_entity
from soweego.wikidata import vocabulary

# Keys
LAST_MODIFIED_KEY = 'last-modified'

PROD_DB_KEY = 'PROD_DB'
TEST_DB_KEY = 'TEST_DB'

DB_ENGINE_KEY = 'DB_ENGINE'
USER_KEY = 'USER'
PASSWORD_KEY = 'PASSWORD'
HOST_KEY = 'HOST'

HANDLED_ENTITIES = {
    'band': 'class',
    'musician': 'occupation',
    'actor': 'occupation',
    'director': 'occupation',
    'producer': 'occupation'
}

# What soweego handles
# TODO add IMDb entities
# TODO add MusicBrainz NLP entities
TARGET_CATALOGS = {
    'discogs': {
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': discogs_entity.DiscogsMusicianEntity,
            'link_entity': discogs_entity.DiscogsMusicianLinkEntity,
            'nlp_entity': discogs_entity.DiscogsMusicianNlpEntity
        },
        'band': {
            'qid': vocabulary.BAND,
            'entity': discogs_entity.DiscogsGroupEntity,
            'link_entity': discogs_entity.DiscogsGroupLinkEntity,
            'nlp_entity': discogs_entity.DiscogsGroupNlpEntity
        }
    },
    'imdb': {
        'actor': {
            'qid': vocabulary.ACTOR,
            'entity': None,
            'link_entity': None,
            'nlp_entity': None
        },
        'director': {
            'qid': vocabulary.FILM_DIRECTOR,
            'entity': None,
            'link_entity': None,
            'nlp_entity': None
        },
        'producer': {
            'qid': vocabulary.FILM_PRODUCER,
            'entity': None,
            'link_entity': None,
            'nlp_entity': None
        }
    },
    'musicbrainz': {
        # FIXME is it correct?
        'musician': {
            'qid': vocabulary.MUSICIAN,
            'entity': musicbrainz_entity.MusicbrainzArtistEntity,
            'link_entity': musicbrainz_entity.MusicbrainzArtistLinkEntity,
            'nlp_entity': None
        },
        'band': {
            'qid': vocabulary.BAND,
            'entity': musicbrainz_entity.MusicbrainzBandEntity,
            'link_entity': musicbrainz_entity.MusicbrainzBandLinkEntity,
            'nlp_entity': None
        }
    }
}
