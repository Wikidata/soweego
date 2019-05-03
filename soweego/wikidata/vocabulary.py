#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of Wikidata vocabulary terms."""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

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
DISCOGS = 'Q504063'
IMDB = 'Q37312'
MUSICBRAINZ = 'Q14005'
TWITTER = 'Q918'

# Identifier properties
DISCOGS_ARTIST_PID = 'P1953'
DISCOGS_MASTER_PID = 'P1954'
IMDB_PID = 'P345'
MUSICBRAINZ_ARTIST_PID = 'P434'
TWITTER_USERNAME_PID = 'P2002'
FACEBOOK_PID = 'P2013'

# Widely used generic property to hold URLs
DESCRIBED_AT_URL = 'P973'
OFFICIAL_WEBSITE = 'P856'

# Entity classes handled by soweego
ACTOR = 'Q33999'
ANIMATOR = 'Q266569'
ART_DIRECTOR = 'Q706364'
ARTIST = 'Q483501'
ASSISTANT_DIRECTOR = 'Q1757008'
BAND = 'Q215380'
CAMERA_OPERATOR = 'Q1208175'
CASTING_DIRECTOR = 'Q1049296'
CASTING_DIRECTOR = 'Q1049296'
CINEMATOGRAPHER = 'Q222344'
COMPOSER = 'Q36834'
COSTUME_DESIGNER = 'Q1323191'
COSTUME_MAKER = 'Q59341113'
DRIVER = 'Q352388'
ELECTRICIAN = 'Q165029'
EXECUTIVE = 'Q978044'
FILM_DIRECTOR = 'Q2526255'
FILM_EDITOR = 'Q7042855'
FILM_PRODUCER = 'Q3282637'
HUMAN = 'Q5'
LOCATION_MANAGER = 'Q1093536'
MAKE_UP_ARTIST = 'Q935666'
MANAGER = 'Q2462658'
MUSICIAN = 'Q639669'
PRODUCTION_ASSISTANT = 'Q2867219'
PRODUCTION_DESIGNER = 'Q2962070'
PRODUCTION_MANAGER = 'Q21292974'
PUBLICIST = 'Q4178004'
SCREENWRITER = 'Q28389'
SCRIPT_SUPERVISOR = 'Q1263187'
SET_DECORATOR = 'Q6409989'
SOUND_DEPARTMENT = 'Q128124'
SPECIAL_EFFECTS = 'Q21560152'
STUNTS = 'Q465501'
TALENT_AGENT = 'Q1344174'
VISUAL_EFFECTS_ARTIST = 'Q1224742'
MUSICALWORK = 'Q2188189'

# Target catalogs helper dictionary
CATALOG_MAPPING = {
    'discogs': {
        'release': {
            'qid': DISCOGS,
            'pid': DISCOGS_MASTER_PID
        },
        'default': {
            'qid': DISCOGS,
            'pid': DISCOGS_ARTIST_PID
        }
    },
    'imdb': {
        'default': {
            'qid': IMDB,
            'pid': IMDB_PID
        }
    },
    'musicbrainz': {
        'default': {
            'qid': MUSICBRAINZ,
            'pid': MUSICBRAINZ_ARTIST_PID
        }
    },
    'twitter': {
        'default': {
            'qid': TWITTER,
            'pid': TWITTER_USERNAME_PID
        }
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
PUBLICATION_DATE = 'P577'
DATE_OF_DEATH = 'P570'
METADATA_PIDS = set([SEX_OR_GENDER, PLACE_OF_BIRTH,
                     PLACE_OF_DEATH, DATE_OF_BIRTH, DATE_OF_DEATH])

# Additional linker properties
BIRTH_NAME = 'P1477'
FAMILY_NAME = 'P734'
GIVEN_NAME = 'P735'
PSEUDONYM = 'P742'

LINKER_PIDS = {
    SEX_OR_GENDER: 'sex_or_gender',
    PLACE_OF_BIRTH: 'place_of_birth',
    PLACE_OF_DEATH: 'place_of_death',
    DATE_OF_BIRTH: 'born',  # Consistent with BaseEntity.born
    DATE_OF_DEATH: 'died',  # Consistent with BaseEntity.died
    BIRTH_NAME: 'birth_name',
    FAMILY_NAME: 'family_name',
    GIVEN_NAME: 'given_name',
    PSEUDONYM: 'pseudonym',
    OCCUPATION: 'occupations',
    PUBLICATION_DATE: 'born'
}

# Music domain properties for linking
MEMBER_OF = 'P463'  # Musician -> bands
HAS_PART = 'P527'  # Band -> musicians
PERFORMER = 'P175'  # Album -> musician/band

# Movie domain
CAST_MEMBER = 'P161'  # Movie -> actor
DIRECTOR = 'P57'  # Movie -> director
PRODUCER = 'P162'  # Movie -> producer

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
IMDB_PROFESSIONS_MAPPINGS = {
    'actor': ACTOR,
    'actress': ACTOR,
    'animation_department': ANIMATOR,
    'art_department': ARTIST,
    'art_director': ART_DIRECTOR,
    'assistant_director': ASSISTANT_DIRECTOR,
    'camera_department': CAMERA_OPERATOR,
    'casting_department': CASTING_DIRECTOR,
    'casting_director': CASTING_DIRECTOR,
    'cinematographer': CINEMATOGRAPHER,
    'composer': COMPOSER,
    'costume_department': COSTUME_MAKER,
    'costume_designer': COSTUME_DESIGNER,
    'director': FILM_DIRECTOR,
    'editor': FILM_EDITOR,
    'electrical_department': ELECTRICIAN,
    'executive': EXECUTIVE,
    'location_management': LOCATION_MANAGER,
    'make_up_department': MAKE_UP_ARTIST,
    'manager': MANAGER,
    'music_department': MUSICIAN,
    'producer': FILM_PRODUCER,
    'production_department': PRODUCTION_ASSISTANT,
    'production_designer': PRODUCTION_DESIGNER,
    'production_manager': PRODUCTION_MANAGER,
    'publicist': PUBLICIST,
    'script_department': SCRIPT_SUPERVISOR,
    'set_decorator': SET_DECORATOR,
    'sound_department': SOUND_DEPARTMENT,
    'soundtrack': MUSICIAN,
    'special_effects': SPECIAL_EFFECTS,
    'stunts': STUNTS,
    'talent_agent': TALENT_AGENT,
    'transportation_department': DRIVER,
    'visual_effects': VISUAL_EFFECTS_ARTIST,
    'writer': SCREENWRITER,
}
