#!/usr/bin/env python3
# coding: utf-8

import urllib.request, urllib.parse, requests
import domain.localizations as loc
import business.utils.file_utils as file_utils

def http_call(base_url, method = 'GET', parameters = None, headers = list()):
    if parameters is not None :
        params = urllib.parse.urlencode(parameters, doseq=True)
        base_url = '{0}?{1}'.format(base_url, params)
    
    #TODO headers implementation

    req = urllib.request.Request(base_url, method=method)

    return urllib.request.urlopen(req)

def download_file(url, filePath):
    try :
        stream = requests.get(url, stream=True)
        with open(filePath, 'wb') as f:
            for chunk in stream.iter_content(chunk_size=1024): 
                if chunk: 
                    f.write(chunk)
    except :
        file_utils.log_error('Unable to download {0}'.format(url))