#!/usr/bin/env python3
# coding: utf-8

"""Utility for files management"""

__author__ = 'Edoardo Lenzi'
__email__ = 'edoardolenzi9@gmail.com'
__version__ = '1.0'
__license__ = 'GPL-3.0'
__copyright__ = 'Copyleft 2018, lenzi.edoardo'

import os
from soweego import __file__


def get_path(package: str, resource: str = None) -> str:
    """From a package-notation path returns the absolute path of the resource"""
    path = os.path.dirname(os.path.abspath(__file__))
    modules = package.split('.')

    for module in modules[1:]:
        path = os.path.join(path, module)

    if resource is not None:
        path = os.path.join(path, resource)

    return path
