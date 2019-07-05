# Contribution Guidelines

## Workflow
1. Fork this repository;
2. follow the project structure;
3. commit **frequently** with **clear** messages;
4. send a pull request to the `master` branch of this repository.

## Coding

### Style
Comply with **[PEP 8](https://www.python.org/dev/peps/pep-0008/)** and **[Wikimedia](https://www.mediawiki.org/wiki/Manual:Coding_conventions/Python)** conventions: 
- _4 spaces_ for indentation;
- _snake-case_ style AKA _underscore_ as a word separator (files, variables, functions);
- _UPPERCASE_ constants;
- anything else is _lowercase_;
- _2_ empty lines to separate functions;
- _80_ characters per line, and up to _100_ when suitable;
- _single-quoted_ strings, unless single-quotes are in a string.

### Type Hints
Add [type hints](https://docs.python.org/3/library/typing.html) at least to public function signatures.

### Documentation
Write _[Sphinx](https://www.sphinx-doc.org/)_ docstrings at least for public functions and classes:
- use [reST](https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html) markup;
- stick to _[PEP 257](https://www.python.org/dev/peps/pep-0257/)_ and _[PEP 287](https://www.python.org/dev/peps/pep-0287/)_;
- pay special attention to [info field lists](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#info-field-lists);
- [cross-reference](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects) Python objects.

### Refactoring
- Fix _[pylint](https://www.pylint.org/)_ errors: `pylint -j 0 -E PATH_TO_YOUR_CONTRIBUTION`;
- look at pylint warnings: `pylint -j 0 -d all -e W PATH_TO_YOUR_CONTRIBUTION`;
- reduce complexity: `flake8 --select C90 --max-complexity 10 PATH_TO_YOUR_CONTRIBUTION`;
- apply relevant refactoring suggestions: `pylint -j 0 -d all -e R PATH_TO_YOUR_CONTRIBUTION`.
