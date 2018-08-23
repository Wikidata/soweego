#!/usr/bin/env python3
# coding: utf-8

import urllib.request
import urllib.parse
import requests

def http_call(base_url, method = 'GET', parameters = None, headers = list()):
    """Makes a generic HTTP call, returns the response"""
    if parameters is not None :
        params = urllib.parse.urlencode(parameters, doseq=True)
        base_url = '{0}?{1}'.format(base_url, params)
    
    #TODO headers implementation

    req = urllib.request.Request(base_url, method=method)

    return urllib.request.urlopen(req)

def download_file(url, filePath):
    """Downloads a web content and saves it in a custom filePath"""
    try :
        stream = requests.get(url, stream=True)
        with open(filePath, 'wb') as f:
            for chunk in stream.iter_content(chunk_size=1024): 
                if chunk: 
                    f.write(chunk)
    except :
        pass # log
        # file_utils.log_error('Unable to download {0}'.format(url))