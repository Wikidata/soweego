from datetime import datetime
from unittest import TestCase, main
import json
from os import environ

environ['PYWIKIBOT_NO_USER_CONFIG'] = '1'

from soweego.importer.models.base_entity import BaseEntity
from soweego.linker import baseline


class BaselineTest(TestCase):

    def test_datecomparison_not_match(self):
        wikidata_json = json.loads("""
        {
            "qid": "Q2032950",
            "name": ["Noah Taylor", "Noah D. Taylor"],
            "born": [
                ["1982-00-00T00:00:00Z", 9]
            ],
            "family_name": ["泰勒", "Τέιλορ", "Тейлор", "テイラー", "Taylor", "Тејлор", "টেইলর", "טיילור", "تايلور"],
            "given_name": ["نواه", "諾亞", "诺亚", "Nhoa", "Noah", "Ноа", "נואה"]
        }""")
        target_entity = BaseEntity()
        target_entity.name = "Noah Taylor"
        target_entity.born = datetime(year=1969, month=9, day=4)
        target_entity.born_precision = 11

        self.assertFalse(baseline.birth_death_date_match(target_entity, wikidata_json))

    def test_datecomparison_maxprecisionmatch(self):
        wikidata_json = json.loads("""
        {
            "qid": "Q2512618",
            "name": ["Roy Gullane"],
            "url": ["https://nl.wikipedia.org/wiki/Roy_Gullane"],
            "born": [
                ["1949-03-19T00:00:00Z", 11]
            ],
            "given_name": ["Рой", "רוי", "洛伊", "Рој", "রায়", "Roy", "روي", "ロイ", "羅伊"]
        }""")
        target_entity = BaseEntity()
        target_entity.name = "Roy Gullane"
        target_entity.born = datetime(year=1949, month=3, day=19)
        target_entity.born_precision = 11
        target_entity.died = None
        target_entity.died_precision = None

        self.assertTrue(baseline.birth_death_date_match(target_entity, wikidata_json))

    def test_datecomparison_bothborndatesnone(self):
        wikidata_json = json.loads("""
        {
            "qid": "Q21402960",
            "name": ["Twinkle", "Baby Twinkle"]
        }""")
        target_entity = BaseEntity()
        target_entity.name = "Twinkle"
        target_entity.born = None
        target_entity.born_precision = None

        self.assertFalse(baseline.birth_death_date_match(target_entity, wikidata_json))

    def test_datecomparison_onedatenone(self):
        wikidata_json = json.loads("""
        {
            "qid": "Q21402960",
            "name": ["Twinkle", "Baby Twinkle"]
        }""")
        target_entity = BaseEntity()
        target_entity.name = "Twinkle"
        target_entity.born = datetime(year=1948, month=7, day=15)
        target_entity.born_precision = 11
        target_entity.died = datetime(year=2015, month=5, day=21)
        target_entity.died_precision = 11

        self.assertFalse(baseline.birth_death_date_match(target_entity, wikidata_json))

    def test_datecomparison_perfectmatch(self):
        wikidata_json = json.loads("""
        {
            "qid": "Q18390650",
            "name": ["Robert Brookins"],
            "born": [
                ["1962-10-07T00:00:00Z", 11]
            ],
            "died": [
                ["2009-04-15T00:00:00Z", 11]
            ]
        }""")

        target_entity = BaseEntity()
        target_entity.name = "Robert Brookins"
        target_entity.born = datetime(year=1962, month=10, day=7)
        target_entity.born_precision = 11
        target_entity.died = datetime(year=2009, month=4, day=15)
        target_entity.died_precision = 11

        self.assertTrue(baseline.birth_death_date_match(target_entity, wikidata_json))

    def test_datecomparison_onlydeathmatch(self):
        wikidata_json = json.loads("""
        {
            "qid": "Q7330088",
            "name": ["Richard Woodward"],
            "born": [
                ["1743-00-00T00:00:00Z", 9]
            ],
            "died": [
                ["1777-11-22T00:00:00Z", 11]
            ]
        }""")
        target_entity = BaseEntity()
        target_entity.name = "Richard Woodward"
        target_entity.born = None
        target_entity.born_precision = None
        target_entity.died = datetime(year=1777, month=11, day=22)
        target_entity.died_precision = 11

        self.assertTrue(baseline.birth_death_date_match(target_entity, wikidata_json))


if __name__ == '__main__':
    main()
