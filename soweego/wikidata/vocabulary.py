#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Wikidata vocabulary terms."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from soweego.commons import keys

# Sandbox items in production site
SANDBOX_1 = 'Q4115189'
SANDBOX_2 = 'Q13406268'
SANDBOX_3 = 'Q15397819'

# Properties used to get instances
INSTANCE_OF = 'P31'
OCCUPATION = 'P106'

# Properties used for references
STATED_IN = 'P248'
RETRIEVED = 'P813'

# Target catalog items
DISCOGS_QID = 'Q504063'
IMDB_QID = 'Q37312'
MUSICBRAINZ_QID = 'Q14005'
TWITTER_QID = 'Q918'

# Identifier properties
DISCOGS_ARTIST_PID = 'P1953'
DISCOGS_MASTER_PID = 'P1954'
IMDB_PID = 'P345'
MUSICBRAINZ_ARTIST_PID = 'P434'
MUSICBRAINZ_RELEASE_GROUP_PID = 'P436'
TWITTER_USERNAME_PID = 'P2002'
FACEBOOK_PID = 'P2013'

# Widely used generic property to hold URLs
DESCRIBED_AT_URL = 'P973'
OFFICIAL_WEBSITE = 'P856'

# Class QID of supported entities
# People
ACTOR_QID = 'Q33999'
ANIMATOR_QID = 'Q266569'
ART_DIRECTOR_QID = 'Q706364'
ARTIST_QID = 'Q483501'
ASSISTANT_DIRECTOR_QID = 'Q1757008'
BAND_QID = 'Q215380'
CAMERA_OPERATOR_QID = 'Q1208175'
CASTING_DIRECTOR_QID = 'Q1049296'
CINEMATOGRAPHER_QID = 'Q222344'
COMPOSER_QID = 'Q36834'
COSTUME_DESIGNER_QID = 'Q1323191'
COSTUME_MAKER_QID = 'Q59341113'
DRIVER_QID = 'Q352388'
ELECTRICIAN_QID = 'Q165029'
EXECUTIVE_QID = 'Q978044'
FILM_DIRECTOR_QID = 'Q2526255'
FILM_EDITOR_QID = 'Q7042855'
FILM_PRODUCER_QID = 'Q3282637'
HUMAN_QID = 'Q5'
LOCATION_MANAGER_QID = 'Q1093536'
MAKE_UP_ARTIST_QID = 'Q935666'
MANAGER_QID = 'Q2462658'
MUSICIAN_QID = 'Q639669'
PRODUCTION_ASSISTANT_QID = 'Q2867219'
PRODUCTION_DESIGNER_QID = 'Q2962070'
PRODUCTION_MANAGER_QID = 'Q21292974'
PUBLICIST_QID = 'Q4178004'
SCREENWRITER_QID = 'Q28389'
SCRIPT_SUPERVISOR_QID = 'Q1263187'
SET_DECORATOR_QID = 'Q6409989'
SOUND_DEPARTMENT_QID = 'Q128124'
SPECIAL_EFFECTS_QID = 'Q21560152'
STUNTS_QID = 'Q465501'
TALENT_AGENT_QID = 'Q1344174'
VISUAL_EFFECTS_ARTIST_QID = 'Q1224742'
# Works
MUSICAL_WORK_QID = 'Q2188189'
AUDIOVISUAL_WORK_QID = 'Q2431196'

# Target catalogs helper dictionary
CATALOG_MAPPING = {
    keys.DISCOGS: {
        keys.CATALOG_QID: DISCOGS_QID,
        keys.PERSON_PID: DISCOGS_ARTIST_PID,
        keys.WORK_PID: DISCOGS_MASTER_PID
    },
    keys.IMDB: {
        keys.CATALOG_QID: IMDB_QID,
        keys.PERSON_PID: IMDB_PID,
        keys.WORK_PID: IMDB_PID
    },
    keys.MUSICBRAINZ: {
        keys.CATALOG_QID: MUSICBRAINZ_QID,
        keys.PERSON_PID: MUSICBRAINZ_ARTIST_PID,
        keys.WORK_PID: MUSICBRAINZ_RELEASE_GROUP_PID
    },
    keys.TWITTER: {
        keys.CATALOG_QID: TWITTER_QID,
        keys.PERSON_PID: TWITTER_USERNAME_PID
    }
}

# Properties with URL data type, from SPARQL query:
# SELECT ?property WHERE { ?property a wikibase:Property ; wikibase:propertyType wikibase:Url . }
URL_PIDS = set([
    'P854', 'P855', 'P856', 'P953', 'P963', 'P968', 'P973', 'P1019', 'P1065',
    'P1324', 'P1325', 'P1348', 'P1401', 'P1421', 'P1482', 'P1581', 'P1613',
    'P1628', 'P1709', 'P1713', 'P1896', 'P1957', 'P1991', 'P2035', 'P2078',
    'P2235', 'P2236', 'P2488', 'P2520', 'P2649', 'P2699', 'P2888', 'P3254',
    'P3268', 'P3950', 'P4001', 'P4238', 'P4570', 'P4656', 'P4765', 'P4945',
    'P4997', 'P5178', 'P5195', 'P5282', 'P5305', 'P5715'
])

# Validator metadata & linker properties: gender, birth/death date/place
SEX_OR_GENDER = 'P21'
PLACE_OF_BIRTH = 'P19'
PLACE_OF_DEATH = 'P20'
DATE_OF_BIRTH = 'P569'
DATE_OF_DEATH = 'P570'
PUBLICATION_DATE = 'P577'
GENRE = 'P136'
METADATA_PIDS = {
    SEX_OR_GENDER, PLACE_OF_BIRTH,
    PLACE_OF_DEATH, DATE_OF_BIRTH, DATE_OF_DEATH
}

# Additional linker properties
BIRTH_NAME = 'P1477'
FAMILY_NAME = 'P734'
GIVEN_NAME = 'P735'
PSEUDONYM = 'P742'

LINKER_PIDS = {
    SEX_OR_GENDER: keys.SEX_OR_GENDER,
    PLACE_OF_BIRTH: keys.PLACE_OF_BIRTH,
    PLACE_OF_DEATH: keys.PLACE_OF_DEATH,
    DATE_OF_BIRTH: keys.DATE_OF_BIRTH,
    DATE_OF_DEATH: keys.DATE_OF_DEATH,
    BIRTH_NAME: keys.BIRTH_NAME,
    FAMILY_NAME: keys.FAMILY_NAME,
    GIVEN_NAME: keys.GIVEN_NAME,
    PSEUDONYM: keys.PSEUDONYM,
    OCCUPATION: keys.OCCUPATIONS,
    GENRE: keys.GENRES,
    PUBLICATION_DATE: keys.DATE_OF_BIRTH
}

# Generic property for work -> person
PARTICIPANT = 'P710'

# Music domain properties for linking
MEMBER_OF = 'P463'  # Musician -> bands
HAS_PART = 'P527'  # Band -> musicians
PERFORMER = 'P175'  # Album -> musician/band

# Movie domain
CAST_MEMBER = 'P161'  # Movie -> actor
DIRECTOR = 'P57'  # Movie -> director
PRODUCER = 'P162'  # Movie -> producer
SCREENWRITER = 'P58'  # Movie -> writer
FILM_CREW_MEMBER = 'P3092'  # Movie -> other occupation
MOVIE_PIDS = (CAST_MEMBER, DIRECTOR, PRODUCER, SCREENWRITER, FILM_CREW_MEMBER)

# Date precision
# See https://www.wikidata.org/wiki/Special:ListDatatypes
BILLION_YEARS = 0
HUNDRED_MILLION_YEARS = 1
TEN_MILLION_YEARS = 2
MILLION_YEARS = 3
HUNDRED_THOUSAND_YEARS = 4
TEN_THOUSAND_YEARS = 5
MILLENNIUM = 6
CENTURY = 7
DECADE = 8
YEAR = 9
MONTH = 10
DAY = 11
HOUR = 12
MINUTE = 13
SECOND = 14
DATE_PRECISION = {
    BILLION_YEARS: 'billion years',
    HUNDRED_MILLION_YEARS: 'hundred million years',
    TEN_MILLION_YEARS: 'ten million years',
    MILLION_YEARS: 'million years',
    HUNDRED_THOUSAND_YEARS: 'hundred thousand years',
    TEN_THOUSAND_YEARS: 'ten thousand years',
    MILLENNIUM: 'millennium',
    CENTURY: 'century',
    DECADE: 'decade',
    YEAR: 'year',
    MONTH: 'month',
    DAY: 'day',
    HOUR: 'hour',
    MINUTE: 'minute',
    SECOND: 'second'
}

# This dictionary provides mappings between the professions
# used by IMDb and their respective Wikidata occupations
IMDB_PROFESSIONS_MAPPING = {
    'actor': ACTOR_QID,
    'actress': ACTOR_QID,
    'animation_department': ANIMATOR_QID,
    'art_department': ARTIST_QID,
    'art_director': ART_DIRECTOR_QID,
    'assistant_director': ASSISTANT_DIRECTOR_QID,
    'camera_department': CAMERA_OPERATOR_QID,
    'casting_department': CASTING_DIRECTOR_QID,
    'casting_director': CASTING_DIRECTOR_QID,
    'cinematographer': CINEMATOGRAPHER_QID,
    'composer': COMPOSER_QID,
    'costume_department': COSTUME_MAKER_QID,
    'costume_designer': COSTUME_DESIGNER_QID,
    'director': FILM_DIRECTOR_QID,
    'editor': FILM_EDITOR_QID,
    'electrical_department': ELECTRICIAN_QID,
    'executive': EXECUTIVE_QID,
    'location_management': LOCATION_MANAGER_QID,
    'make_up_department': MAKE_UP_ARTIST_QID,
    'manager': MANAGER_QID,
    'music_department': MUSICIAN_QID,
    'producer': FILM_PRODUCER_QID,
    'production_department': PRODUCTION_ASSISTANT_QID,
    'production_designer': PRODUCTION_DESIGNER_QID,
    'production_manager': PRODUCTION_MANAGER_QID,
    'publicist': PUBLICIST_QID,
    'script_department': SCRIPT_SUPERVISOR_QID,
    'set_decorator': SET_DECORATOR_QID,
    'sound_department': SOUND_DEPARTMENT_QID,
    'soundtrack': MUSICIAN_QID,
    'special_effects': SPECIAL_EFFECTS_QID,
    'stunts': STUNTS_QID,
    'talent_agent': TALENT_AGENT_QID,
    'transportation_department': DRIVER_QID,
    'visual_effects': VISUAL_EFFECTS_ARTIST_QID,
    'writer': SCREENWRITER_QID
}

# Used to populate statements on works
WORKS_BY_PEOPLE_MAPPING = {
    keys.DISCOGS: {
        keys.BAND: PERFORMER,
        keys.MUSICIAN: PERFORMER
    },
    keys.IMDB: {
        keys.ACTOR: PARTICIPANT,
        keys.DIRECTOR: PARTICIPANT,
        keys.MUSICIAN: PARTICIPANT,
        keys.PRODUCER: PARTICIPANT,
        keys.WRITER: PARTICIPANT
    },
    keys.MUSICBRAINZ: {
        keys.BAND: PERFORMER,
        keys.MUSICIAN: PERFORMER
    }
}
