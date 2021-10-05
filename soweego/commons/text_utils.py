#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Set of standard NLP utilities for text processing"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

import logging
import re
from pkgutil import get_data

LOGGER = logging.getLogger(__name__)

# Adapted from http://snowball.tartarus.org/algorithms/english/stop.txt
STOPWORDS_ENG = frozenset(
    str(get_data('soweego.commons.resources', 'stopwords_eng.txt'), 'utf8').splitlines()
)
COMMON_WORDS_ENG = frozenset(
    str(
        get_data('soweego.commons.resources', 'common_words_eng.txt'), 'utf8'
    ).splitlines()
)

NAME_STOPWORDS = frozenset(
    str(
        get_data('soweego.commons.resources', 'name_stopwords.txt'), 'utf8'
    ).splitlines()
)

BAND_NAME_LOW_SCORE_WORDS = frozenset(
    str(get_data('soweego.commons.resources', 'band_low_score_words.txt')).splitlines()
)

STOPWORDS_URL_TOKENS = frozenset(
    str(
        get_data('soweego.commons.resources', 'urls_stop_words.txt'), 'utf8'
    ).splitlines()
)

# Latin alphabet diacritics and Russian
ASCII_TRANSLATION_TABLE = str.maketrans(
    {
        'á': 'a',
        'Á': 'A',
        'à': 'a',
        'À': 'A',
        'ă': 'a',
        'Ă': 'A',
        'â': 'a',
        'Â': 'A',
        'å': 'a',
        'Å': 'A',
        'ã': 'a',
        'Ã': 'A',
        'ą': 'a',
        'Ą': 'A',
        'ā': 'a',
        'Ā': 'A',
        'ä': 'ae',
        'Ä': 'AE',
        'æ': 'ae',
        'Æ': 'AE',
        'ḃ': 'b',
        'Ḃ': 'B',
        'ć': 'c',
        'Ć': 'C',
        'ĉ': 'c',
        'Ĉ': 'C',
        'č': 'c',
        'Č': 'C',
        'ċ': 'c',
        'Ċ': 'C',
        'ç': 'c',
        'Ç': 'C',
        'ď': 'd',
        'Ď': 'D',
        'ḋ': 'd',
        'Ḋ': 'D',
        'đ': 'd',
        'Đ': 'D',
        'ð': 'dh',
        'Ð': 'Dh',
        'é': 'e',
        'É': 'E',
        'è': 'e',
        'È': 'E',
        'ĕ': 'e',
        'Ĕ': 'E',
        'ê': 'e',
        'Ê': 'E',
        'ě': 'e',
        'Ě': 'E',
        'ë': 'e',
        'Ë': 'E',
        'ė': 'e',
        'Ė': 'E',
        'ę': 'e',
        'Ę': 'E',
        'ē': 'e',
        'Ē': 'E',
        'ḟ': 'f',
        'Ḟ': 'F',
        'ƒ': 'f',
        'Ƒ': 'F',
        'ğ': 'g',
        'Ğ': 'G',
        'ĝ': 'g',
        'Ĝ': 'G',
        'ġ': 'g',
        'Ġ': 'G',
        'ģ': 'g',
        'Ģ': 'G',
        'ĥ': 'h',
        'Ĥ': 'H',
        'ħ': 'h',
        'Ħ': 'H',
        'í': 'i',
        'Í': 'I',
        'ì': 'i',
        'Ì': 'I',
        'î': 'i',
        'Î': 'I',
        'ï': 'i',
        'Ï': 'I',
        'ĩ': 'i',
        'Ĩ': 'I',
        'į': 'i',
        'Į': 'I',
        'ī': 'i',
        'Ī': 'I',
        'ĵ': 'j',
        'Ĵ': 'J',
        'ķ': 'k',
        'Ķ': 'K',
        'ĺ': 'l',
        'Ĺ': 'L',
        'ľ': 'l',
        'Ľ': 'L',
        'ļ': 'l',
        'Ļ': 'L',
        'ł': 'l',
        'Ł': 'L',
        'ṁ': 'm',
        'Ṁ': 'M',
        'ń': 'n',
        'Ń': 'N',
        'ň': 'n',
        'Ň': 'N',
        'ñ': 'n',
        'Ñ': 'N',
        'ņ': 'n',
        'Ņ': 'N',
        'ó': 'o',
        'Ó': 'O',
        'ò': 'o',
        'Ò': 'O',
        'ô': 'o',
        'Ô': 'O',
        'ő': 'o',
        'Ő': 'O',
        'õ': 'o',
        'Õ': 'O',
        'ø': 'oe',
        'Ø': 'OE',
        'ō': 'o',
        'Ō': 'O',
        'ơ': 'o',
        'Ơ': 'O',
        'ö': 'oe',
        'Ö': 'OE',
        'ṗ': 'p',
        'Ṗ': 'P',
        'ŕ': 'r',
        'Ŕ': 'R',
        'ř': 'r',
        'Ř': 'R',
        'ŗ': 'r',
        'Ŗ': 'R',
        'ś': 's',
        'Ś': 'S',
        'ŝ': 's',
        'Ŝ': 'S',
        'š': 's',
        'Š': 'S',
        'ṡ': 's',
        'Ṡ': 'S',
        'ş': 's',
        'Ş': 'S',
        'ș': 's',
        'Ș': 'S',
        'ß': 'SS',
        'ť': 't',
        'Ť': 'T',
        'ṫ': 't',
        'Ṫ': 'T',
        'ţ': 't',
        'Ţ': 'T',
        'ț': 't',
        'Ț': 'T',
        'ŧ': 't',
        'Ŧ': 'T',
        'ú': 'u',
        'Ú': 'U',
        'ù': 'u',
        'Ù': 'U',
        'ŭ': 'u',
        'Ŭ': 'U',
        'û': 'u',
        'Û': 'U',
        'ů': 'u',
        'Ů': 'U',
        'ű': 'u',
        'Ű': 'U',
        'ũ': 'u',
        'Ũ': 'U',
        'ų': 'u',
        'Ų': 'U',
        'ū': 'u',
        'Ū': 'U',
        'ư': 'u',
        'Ư': 'U',
        'ü': 'ue',
        'Ü': 'UE',
        'ẃ': 'w',
        'Ẃ': 'W',
        'ẁ': 'w',
        'Ẁ': 'W',
        'ŵ': 'w',
        'Ŵ': 'W',
        'ẅ': 'w',
        'Ẅ': 'W',
        'ý': 'y',
        'Ý': 'Y',
        'ỳ': 'y',
        'Ỳ': 'Y',
        'ŷ': 'y',
        'Ŷ': 'Y',
        'ÿ': 'y',
        'Ÿ': 'Y',
        'ź': 'z',
        'Ź': 'Z',
        'ž': 'z',
        'Ž': 'Z',
        'ż': 'z',
        'Ż': 'Z',
        'þ': 'th',
        'Þ': 'Th',
        'µ': 'u',
        'а': 'a',
        'А': 'a',
        'б': 'b',
        'Б': 'b',
        'в': 'v',
        'В': 'v',
        'г': 'g',
        'Г': 'g',
        'д': 'd',
        'Д': 'd',
        'е': 'e',
        'Е': 'E',
        'ё': 'e',
        'Ё': 'E',
        'ж': 'zh',
        'Ж': 'zh',
        'з': 'z',
        'З': 'z',
        'и': 'i',
        'И': 'i',
        'й': 'j',
        'Й': 'j',
        'к': 'k',
        'К': 'k',
        'л': 'l',
        'Л': 'l',
        'м': 'm',
        'М': 'm',
        'н': 'n',
        'Н': 'n',
        'о': 'o',
        'О': 'o',
        'п': 'p',
        'П': 'p',
        'р': 'r',
        'Р': 'r',
        'с': 's',
        'С': 's',
        'т': 't',
        'Т': 't',
        'у': 'u',
        'У': 'u',
        'ф': 'f',
        'Ф': 'f',
        'х': 'h',
        'Х': 'h',
        'ц': 'c',
        'Ц': 'c',
        'ч': 'ch',
        'Ч': 'ch',
        'ш': 'sh',
        'Ш': 'sh',
        'щ': 'sch',
        'Щ': 'sch',
        'ъ': '',
        'Ъ': '',
        'ы': 'y',
        'Ы': 'y',
        'ь': '',
        'Ь': '',
        'э': 'e',
        'Э': 'e',
        'ю': 'ju',
        'Ю': 'ju',
        'я': 'ja',
        'Я': 'ja',
    }
)


def tokenize(text, stopwords=STOPWORDS_ENG):
    """:func:`Normalize` and tokenize a text."""
    tokens = set()
    ascii_only, ascii_lowercase = normalize(text)
    split = re.split(r'\W+', ascii_lowercase)
    filtered = filter(lambda token: len(token) > 1, split)
    for token in filtered:
        if token and token not in stopwords:
            tokens.add(token)
    LOGGER.debug(
        'Tokenization pipeline: INPUT --> %s --> ASCII --> %s --> LOWERCASE --> %s --> SPLIT --> %s --> NO 0/1-GRAMS + NO STOPWORDS --> %s',
        text,
        ascii_only,
        ascii_lowercase,
        split,
        tokens,
    )
    if not tokens:
        LOGGER.debug("No tokens from text '%s'", text)
    return tokens


def normalize(text):
    """Strip, convert to ASCII and lowercase a text."""
    ascii_only = text.strip().translate(ASCII_TRANSLATION_TABLE)
    ascii_lowercase = ascii_only.lower()
    return ascii_only, ascii_lowercase
