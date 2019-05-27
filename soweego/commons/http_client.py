#!/usr/bin/env python3
# coding: utf-8

"""Utility for http calls/connections management"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import logging
import ssl
import urllib

import requests
import urllib3
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)


def http_call(base_url, method='GET', parameters=None, headers=list()):
    """Makes a generic HTTP call, returns the response"""
    if parameters is not None:
        params = urllib.parse.urlencode(parameters, doseq=True)
        base_url = '{0}?{1}'.format(base_url, params)

    # TODO headers implementation
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    ssl._create_default_https_context = ssl._create_unverified_context
    req = urllib.request.Request(base_url, method=method)
    return urllib.request.urlopen(req)


def download_file(url, filePath):
    """Downloads a web content and saves it in a custom filePath"""
    try:
        file_size = int(requests.head(url).headers["Content-Length"])
        pbar = tqdm(
            total=file_size,
            unit='B', unit_scale=True, desc=url.split('/')[-1])

        stream = requests.get(url, stream=True, verify=False)
        with open(filePath, 'wb') as f:
            for chunk in stream.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    pbar.update(1024)
                    
            pbar.close()

    except Exception as e:
        LOGGER.warning('Unable to download %s \n %s', url, str(e))
