# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
import os
import sys
sys.path.insert(0, os.path.abspath('..'))


# -- Project information -----------------------------------------------------

project = 'soweego'
copyright = 'MMXIX, <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">Marco Fossati</a>. A <a href="https://wikimediafoundation.org/">Wikimedia Foundation</a> project'
author = 'Marco Fossati, Massimo Frasson, Edoardo Lenzi, Andrea Tupini'

# The full version, including alpha/beta/rc tags
release = '1.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx_autodoc_typehints',
    'sphinx_click.ext'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
# https://alabaster.readthedocs.io/en/latest/customization.html#theme-options
html_theme_options = {
    # Basic
    'page_width': '980px',
    'note_bg': '#9CF',
    # Sidebar
    'logo': 'logo.png',
    'logo_name': True,
    'logo_text_align': 'center',
    'description': 'Link Wikidata to large catalogs',
    'sidebar_collapse': True,
    # Services & badges
    'github_user': 'Wikidata',
    'github_repo': 'soweego',
    'github_banner': True,
    'github_button': True,
    'github_type': 'star',
    'github_count': False,
    # Header & footer
    'show_powered_by': False,

}

# -- Extension configuration -------------------------------------------------

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'recordlinkage': ('https://recordlinkage.readthedocs.io/en/latest/', None),
    'requests': ('https://2.python-requests.org/en/stable/', None),
    'sqlalchemy': ('https://docs.sqlalchemy.org/en/13/', None),
    'sklearn': ('https://scikit-learn.org/stable/', None),
}
