#!/usr/bin/env python3
# coding: utf-8

"""TODO docstring"""


class DumpState(object):
    output_path: str 
    download_url: str
    last_modified: str


    def __init__(self, output_path, download_url, last_modified = None):
        self.output_path = output_path
        self.download_url = download_url
        self.last_modified = last_modified


class Mappings(object):
    table_name: str
    source_name: str

    def __init__(self, table_name, source_name):
        self.table_name = table_name
        self.source_name = source_name


#class Dumps(object):
#    dump: DumpState
#    mappings: [] 


#    def __init__(self, dump, mappings):
#        self.dump = dump
#        self.mappings = mappings
