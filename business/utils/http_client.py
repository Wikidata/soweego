#!/usr/bin/python

import urllib, urllib2, json, re, requests
from collections import namedtuple
import domain.localizations as loc
import business.utils.file_utils as file_utils
from requests.utils import quote

def json_deserialize(serialized_json):
    return json.loads(serialized_json, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

def http_call(base_url, method = 'GET', parameters = None, headers = []):
    if parameters is not None :
        params = urllib.urlencode(parameters, doseq=True)
        base_url = '{0}?{1}'.format(base_url, params)
    request = urllib2.Request(base_url)
    request.get_method = lambda: method
    for header in headers :
        request.add_header(header.key, header.value)
    return urllib2.urlopen(request)

def download_file(url, filePath):
    try :
        stream = requests.get(url, stream=True)
        with open(filePath, 'wb') as f:
            for chunk in stream.iter_content(chunk_size=1024): 
                if chunk: 
                    f.write(chunk)
    except :
        file_utils.log_error('Unable to download {0}'.format(url))

def build_query_url(base_url, query_parameters):
    url = '{0}?'.format(base_url)
    for key, value in query_parameters.iteritems():
        url = '{0}{1}={2}&'.format(url, key, percent_encoding(value))
    return url[:-1]

def percent_encoding(query):
    return quote(query, safe='')
