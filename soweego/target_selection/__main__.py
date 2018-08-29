#!/usr/bin/env python3
# coding: utf-8

# Launch point for all the subpackage scripts

#import targets.musicbrainz.musicbrainz_baseline_matcher as mb

#mb.equal_strings_match()

from .commons import constants as const 
from .commons.models.orm.bibsys_entity import BibsysEntity
from .commons.models.orm.bibsys_reference import BibsysReference
from .commons.models.orm.domain import Domain 
from soweego.target_selection.commons.utils import json_utils

configs = json_utils.load_json(const.cofigs_path)

# Creates DB Schema
if configs[const.create_db_schema_key]:
    BibsysEntity().create_table()
    BibsysReference().create_table()
    Domain().create_table()
