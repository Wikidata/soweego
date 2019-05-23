#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Dump extractor abstract class"""
import logging

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

from typing import Iterable

LOGGER = logging.getLogger(__name__)


class BaseDumpExtractor:
    """Defines where to download a certain dump and how to post-process it."""

    def extract_and_populate(self, dump_file_path: Iterable[str],
                             resolve: bool):
        """Extract relevant data and populate SQL Alchemy entities accordingly.

        :param dump_file_path: Iterable of paths where downloaded dumps are
        placed.
        :param resolve: Tells if the system will resolve the urls to validate
        them.
        """
        raise NotImplementedError

    def get_dump_download_urls(self) -> Iterable[str]:
        """Get the dump download URL.
        Useful if there is a way to compute the latest dump URL.

        :return: the latest dumps URL
        :rtype: Iterable[str]
        """
        raise NotImplementedError
