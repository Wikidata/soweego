**WORK IN PROGRESS**

# soweego
_soweego_ is an entity linking system that connects Wikidata items to trusted external catalogs.

# Official Project Page
https://meta.wikimedia.org/wiki/Grants:Project/Hjfocs/soweego

# Development Policy
Contributors should follow the standard team development practices:
1. branch out of `master`;
2. follow the project structure;
3. commit **frequently** with **clear** messages;
4. make a pull request.

# Coding Style
- Use _[pylint](https://www.pylint.org/)_ with default rules (for now), see `.pylint.rc`;
- Use _4 spaces_ (soft tab) for indentation;
- Naming conventions
  - use an _underscore_ as a word separator (files, variables, functions);
  - constants are _UPPERCASE_;
  - anything else is _lowercase_.
- Use _2_ empty lines to separate functions;
- Write docstrings according to _[PEP 287](https://www.python.org/dev/peps/pep-0287/)_, with a special attention to [field lists](http://sphinx-doc.org/domains.html#info-field-lists).
