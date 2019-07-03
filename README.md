# soweego: link Wikidata to large catalogs
[![Build Status](https://travis-ci.com/Wikidata/soweego.svg?branch=master)](https://travis-ci.com/Wikidata/soweego)
[![Documentation Status](https://readthedocs.org/projects/soweego/badge/?version=latest)](https://soweego.readthedocs.io/en/latest/?badge=latest)

_soweego_ is a pipeline that connects [Wikidata](https://wikidata.org/) to large-scale third-party catalogs.

_soweego_ is the only system that makes _statisticians, epidemiologists, historians,_ and _computer scientists_ agree.
Why? Because it performs _record linkage, data matching,_ and _entity resolution_ at the same time.
Too easy, they all seem to be [synonyms](https://en.wikipedia.org/wiki/Record_linkage#Naming_conventions)!

Oh, _soweego_ also embeds [Machine Learning](https://en.wikipedia.org/wiki/Machine_learning) and advocates for [Linked Data](https://en.wikipedia.org/wiki/Linked_data).

![Is soweego similar to the Go game?](https://upload.wikimedia.org/wikipedia/commons/9/96/Crosscut.jpg)

## Official Project Page
https://meta.wikimedia.org/wiki/Grants:Project/Hjfocs/soweego

## Documentation
https://soweego.readthedocs.io/

## Highlights
- Run the whole [pipeline](#Run_the_Pipeline), or
- use the [command line](#Command_Line);
- [import](https://soweego.readthedocs.io/en/latest/importer.html) large catalogs into a SQL database;
- [gather](https://soweego.readthedocs.io/en/latest/wikidata.html) live Wikidata datasets;
- [connect](https://soweego.readthedocs.io/en/latest/linker.html) them to target catalogs via _rule-based_ and _supervised_ linkers;
- [upload](https://soweego.readthedocs.io/en/latest/ingestor.html) links to Wikidata and [Mix'n'match](https://tools.wmflabs.org/mix-n-match/);
- [synchronize](https://soweego.readthedocs.io/en/latest/validator.html) Wikidata to imported catalogs;
- [enrich]() Wikidata items with statements

## Get Ready
Install [Docker](https://docs.docker.com/install/), then grab soweego:

```bash
$ git clone https://github.com/Wikidata/soweego.git
$ cd soweego
```

## Run the Pipeline
Piece of cake:

```bash
$ ./scripts/docker/launch_pipeline.sh CATALOG
```

Pick `CATALOG` from: `discogs`, `imdb`, `musicbrainz`.

These steps are executed by default:
1. import the target catalog into a database;
2. link Wikidata to the target with a supervised linker;
3. synchronize Wikidata to the target.

## Use the Command Line
You can launch every single soweego action with CLI commands:

```bash
$ ./scripts/docker/launch_test.sh
Building soweego
# cd soweego
# python -m soweego
Usage: soweego [OPTIONS] COMMAND [ARGS]...

  Link Wikidata to large catalogs.

Options:
  -l, --log-level <TEXT CHOICE>...
                                  Module name followed by one of [DEBUG, INFO,
                                  WARNING, ERROR, CRITICAL]. Multiple pairs
                                  allowed.
  --help                          Show this message and exit.

Commands:
  importer  Import target catalog dumps into the database.
  ingest    Take soweego output into Wikidata items.
  linker    Link Wikidata items to target catalog identifiers.
  run       Launch the whole pipeline.
  sync      Sync Wikidata to target catalogs.
```

Just two things to remember:
1. you can always get `--help`;
2. each command may have sub-commands.

## Contribute
The best way is to [add a new catalog](https://github.com/Wikidata/soweego/wiki/Import-a-new-database).

### Workflow
1. branch out of `master`;
2. follow the project structure;
3. commit **frequently** with **clear** messages;
4. make a pull request.

### Coding
1. **Style** - comply with **[PEP 8](https://www.python.org/dev/peps/pep-0008/)** and **[Wikimedia](https://www.mediawiki.org/wiki/Manual:Coding_conventions/Python)** conventions: 
  - _4 spaces_ for indentation;
  - _snake-case_ style AKA _underscore_ as a word separator (files, variables, functions);
  - _UPPERCASE_ constants;
  - anything else is _lowercase_;
  - _2_ empty lines to separate functions;
  - _80_ characters per line.
2. **[Type hints](https://docs.python.org/3/library/typing.html)** - add them to public function signatures;
2. **Documentation** - write _[Sphinx](https://www.sphinx-doc.org/)_ docstrings for public functions and classes:
  - use [reST](https://www.sphinx-doc.org/en/master/usage/restructuredtext/index.html)
  - follow _[PEP 257](https://www.python.org/dev/peps/pep-0257/)_ and _[PEP 287](https://www.python.org/dev/peps/pep-0287/)_;
  - pay special attention to [info field lists](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#info-field-lists);
  - [cross-reference](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects) Python objects.
3. **Refactoring**
- fix _[pylint](https://www.pylint.org/)_ errors: `pylint -j 0 -E soweego`;
- look at pylint warnings: `pylint -j 0 -d all -e W soweego`;
- reduce complexity: `flake8 --select C90 --max-complexity 10 soweego`;
- apply relevant refactoring suggestions: `pylint -j 0 -d all -e R soweego`.

## License
The source code is under the terms of the [GNU General Public License, version 3](http://www.gnu.org/licenses/gpl.html).
