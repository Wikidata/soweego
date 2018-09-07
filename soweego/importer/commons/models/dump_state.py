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
