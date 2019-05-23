![Work in progress](https://mauroparravicini.files.wordpress.com/2011/10/lavori_in_corso.jpg?w=150&h=131)

**Work in progress!**

[![Build Status](https://travis-ci.com/Wikidata/soweego.svg?branch=master)](https://travis-ci.com/Wikidata/soweego)

# soweego
_soweego_ is an entity linking system that connects Wikidata items to trusted external catalogs.

# Official Project Page
https://meta.wikimedia.org/wiki/Grants:Project/Hjfocs/soweego

# Development Policy
Contributors should comply with these steps:
1. branch out of `master`;
2. follow the project structure;
3. commit **frequently** with **clear** messages;
4. make a pull request.

_[pipenv](https://docs.python-guide.org/dev/virtualenvs/#installing-pipenv)_ is highly recommended.

# Coding Style
- Comply with **[PEP 8](https://www.python.org/dev/peps/pep-0008/)** and **[Wikimedia](https://www.mediawiki.org/wiki/Manual:Coding_conventions/Python)** conventions;
- use _[pylint](https://www.pylint.org/)_ with rules as per `.pylintrc`:
  - _4 spaces_ (soft tab) for indentation;
  - _snake-case_ style, i.e., _underscore_ as a word separator (files, variables, functions);
  - _UPPERCASE_ constants;
  - anything else is _lowercase_;
  - _2_ empty lines to separate functions;
- write _[Sphinx](http://www.sphinx-doc.org/en/stable/)_ docstrings:
  - follow _[PEP 257](https://www.python.org/dev/peps/pep-0257/)_ and _[PEP 287](https://www.python.org/dev/peps/pep-0287/)_;
  - pay special attention to [field lists](http://sphinx-doc.org/domains.html#info-field-lists).
