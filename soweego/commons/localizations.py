#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Localization keys"""

# Exceptions
MISSING_CREDENTIALS = 'DB Credentials or Configs not found'
FAIL_CREATE_ENGINE = (
    'Failed to create the DB engine, please check your credentials. Reason: %s'
)
FAIL_DOWNLOAD = 'Fails on dump download'
FAIL_HANDLER = 'Handler fails on dump scraping'
MALFORMED_ROW = 'Malformed Row, brokes the structure <subject> <predicate> <object>'
FIELD_NOT_MAPPED = 'Field: \t %s \t not mapped'
WRONG_MAPPINGS = 'Errors at DB import, probably due to wrong mappings \n %s'
