#!/usr/bin/env python3
# coding: utf-8

class DumpState(object):
    name = None 
    location = None 
    extension = None 
    last_modified = None 
    file_name = None
    base_uri = None
    rdf_type = None
    rdf_person = None

    def __init__(self, name, location, extension, base_uri, rdf_type, rdf_person, last_modified = None):
        self.name = name
        self.location = location
        self.extension = extension
        self.last_modified = last_modified
        self.base_uri = base_uri
        self.file_name = '{0}.{1}'.format(self.name, self.extension)
        self.rdf_person = rdf_person
        self.rdf_type = rdf_type
