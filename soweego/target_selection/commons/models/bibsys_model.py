#!/usr/bin/env python3
# coding: utf-8

import datetime

class Bibsys(object):
    identifier : int
    name: str
    since : int 
    modified : datetime
    until : int 
    is_person : bool
    catalogue_name : str
    suffix_name : str
    alt_label : str
    label : str
    note : str
    same_as : [] 

    def __init__(self, identifier = None, name = None, since = None, until = None, is_person = False, same_as = []):
        self.name = name
        self.identifier = identifier
        self.since = since
        self.until = until
        self.is_person = is_person
        self.same_as = same_as
