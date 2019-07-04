Workflow
--------

1. Fork this repository;
2. follow the project structure;
3. commit **frequently** with **clear** messages;
4. send a pull request to the ``master`` branch of this repository.


Coding
------

1. **Style** - comply with `PEP 8 <https://www.python.org/dev/peps/pep-0008/>`_ and `Wikimedia <https://www.mediawiki.org/wiki/Manual:Coding_conventions/Python>`_ conventions:

    - *4 spaces* for indentation;
    - *snake-case* style AKA *underscore* as a word separator (files, variables, functions);
    - *UPPERCASE* constants;
    - anything else is *lowercase*;
    - *2* empty lines to separate functions;
    - *80* characters per line, and up to *100* when suitable;
    - *single-quoted* strings, unless single-quotes are in a string.

2. `Type hints <https://docs.python.org/3/library/typing.html>`_ - add them at least to public function signatures;
3. **Documentation** - write `Sphinx <https://www.sphinx-doc.org/>`_ docstrings at least for public functions and classes:

    - use `reST <https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html>`_ markup;
    - stick to `PEP 257 <https://www.python.org/dev/peps/pep-0257/>`_ and _`PEP 287 <https://www.python.org/dev/peps/pep-0287/>`_;
    - pay special attention to `info field lists <https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#info-field-lists>`_;
    - `cross-reference <https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects>`_ Python objects.

4. **Refactoring**:

    - fix `pylint <https://www.pylint.org/>`_ errors: ``pylint -j 0 -E PATH_TO_YOUR_CONTRIBUTION``;
    - look at pylint warnings: ``pylint -j 0 -d all -e W PATH_TO_YOUR_CONTRIBUTION``;
    - reduce complexity: ``flake8 --select C90 --max-complexity 10 PATH_TO_YOUR_CONTRIBUTION``;
    - apply relevant refactoring suggestions: ``pylint -j 0 -d all -e R PATH_TO_YOUR_CONTRIBUTION``.
