#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Base class for catalog dumps extraction."""

import logging
import warnings
from typing import List, Optional

__author__ = 'Marco Fossati'
__email__ = 'fossati@spaziodati.eu'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, Hjfocs'

LOGGER = logging.getLogger(__name__)

# Silence full-text index creation warning
warnings.filterwarnings(
    'ignore', message='.*rebuilding table to add column FTS_DOC_ID.*'
)


class BaseDumpExtractor:
    """Method definitions to download catalog dumps, extract data, and
    populate a database instance.
    """

    def extract_and_populate(
        self, dump_file_paths: List[str], resolve: bool
    ) -> None:
        """Extract relevant data and populate
        `SQLAlchemy <https://www.sqlalchemy.org/>`_ ORM entities accordingly.
        Entities will be then persisted to a database instance.

        :param dump_file_paths: paths to downloaded catalog dumps
        :param resolve: whether to resolve URLs found in catalog dumps or not
        """
        raise NotImplementedError

    def get_dump_download_urls(self) -> Optional[List[str]]:
        """Get the dump download URLs of a target catalog.
        Useful if there is a way to compute the latest URLs.

        :return: the latest dumps URL
        """
        raise NotImplementedError
