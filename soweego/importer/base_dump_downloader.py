#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Dump downloader interface"""

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'


class BaseDumpDownloader:

    def import_from_dump(self, dump_file_path):
        """Given the path to a downloaded dump file, 
        this method is called to perform the import into the database

        Raises:
            NotImplementedError -- You have to override this method
        """
        raise NotImplementedError

    def dump_download_url(self) -> str:
        """Implement this method to return a computed dump URL.
        Useful if there is a way to compute the latest dump URL.

        Raises:
            NotImplementedError -- You can avoid the override, it's not an issue
        """
        raise NotImplementedError
