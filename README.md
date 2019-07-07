# soweego: link Wikidata to large catalogs
[![Build Status](https://travis-ci.com/Wikidata/soweego.svg?branch=master)](https://travis-ci.com/Wikidata/soweego)
[![Documentation Status](https://readthedocs.org/projects/soweego/badge/?version=latest)](https://soweego.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/github/license/Wikidata/soweego.svg)](https://www.gnu.org/licenses/gpl-3.0.html)

_soweego_ is a pipeline that connects [Wikidata](https://wikidata.org/) to large-scale third-party catalogs.

_soweego_ is the only system that makes _statisticians, epidemiologists, historians,_ and _computer scientists_ agree.
Why? Because it performs _record linkage, data matching,_ and _entity resolution_ at the same time.
Too easy, they all seem to be [synonyms](https://en.wikipedia.org/wiki/Record_linkage#Naming_conventions)!

Oh, _soweego_ also embeds [Machine Learning](https://en.wikipedia.org/wiki/Machine_learning) and advocates for [Linked Data](https://en.wikipedia.org/wiki/Linked_data).

![Is soweego similar to the Go game?](https://upload.wikimedia.org/wikipedia/commons/9/96/Crosscut.jpg)

# Official Project Page
https://meta.wikimedia.org/wiki/Grants:Project/Hjfocs/soweego

# Documentation
https://soweego.readthedocs.io/

# Highlights
- Run the whole [pipeline](#Run_the_Pipeline), or
- use the [command line](#Command_Line);
- [import](https://soweego.readthedocs.io/en/latest/importer.html) large catalogs into a SQL database;
- [gather](https://soweego.readthedocs.io/en/latest/wikidata.html) live Wikidata datasets;
- [connect](https://soweego.readthedocs.io/en/latest/linker.html) them to target catalogs via _rule-based_ and _supervised_ linkers;
- [upload](https://soweego.readthedocs.io/en/latest/ingester.html) links to Wikidata and [Mix'n'match](https://tools.wmflabs.org/mix-n-match/);
- [synchronize](https://soweego.readthedocs.io/en/latest/validator.html#module-soweego.validator.checks) Wikidata to imported catalogs;
- [enrich](https://soweego.readthedocs.io/en/latest/validator.html#module-soweego.validator.enrichment) Wikidata items with relevant statements.

# Get Ready
Install [Docker](https://docs.docker.com/install/), then grab soweego:

```
$ git clone https://github.com/Wikidata/soweego.git
$ cd soweego
```

# Run the Pipeline
Piece of cake:

```
$ ./docker/launch_pipeline.sh CATALOG
Building soweego
...

:/app/soweego# python -m soweego run CATALOG
```

Pick `CATALOG` from `discogs`, `imdb`, or `musicbrainz`.

These steps are executed by default:
1. import the target catalog into a database;
2. link Wikidata to the target with a supervised linker;
3. synchronize Wikidata to the target.

Results are in `/app/shared/results`.

# Use the Command Line
You can launch every single soweego action with CLI commands:

```
$ ./docker/run.sh
Building soweego
...

:/app/soweego# python -m soweego
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
  ingester  Take soweego output into Wikidata items.
  linker    Link Wikidata items to target catalog identifiers.
  run       Launch the whole pipeline.
  sync      Sync Wikidata to target catalogs.
```

Just two things to remember:
1. you can always get `--help`;
2. each command may have sub-commands.

# Contribute
The best way is to [add a new catalog](https://github.com/Wikidata/soweego/wiki/Import-a-new-database).
Have a look at the [guidelines](CONTRIBUTING.md).

# License
The source code is under the terms of the [GNU General Public License, version 3](https://www.gnu.org/licenses/gpl.html).
