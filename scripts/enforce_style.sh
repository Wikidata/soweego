#!/usr/bin/env bash

# This script contains a set of commands to enforce
# standard style guidelines and design patterns for Python code.
#
# Each command writes a report file: it is recommended to inspect
# one file at a time and fix everything that seems reasonable.
# File names start with a number that tells you the order of inspection.

USAGE="Usage: $(basename "$0") PYTHON_FILE_OR_DIR"
if [[ $# -ne 1 ]]; then
        echo $USAGE
        exit 1
fi

WD=$(pwd)
OUTDIR="$WD/style_reports"

if [[ ! -d $OUTDIR ]]; then
    mkdir $OUTDIR
fi

# sphinx warnings
echo "Step 1: sphinx documentation warnings ..."
cd "$WD/docs" && make html > "$OUTDIR/01_sphinx_warnings" && cd $WD

# pylint errors
echo "Step 2: pylint errors ..."
pylint -j 0 -E $1 > "$OUTDIR/02_pylint_errors"

# pylint warnings
echo "Step 3: pylint warnings ..."
pylint -j 0 -d all -e W $1 > "$OUTDIR/03_pylint_warnings"

# mccabe complexity
echo "Step 4: mccabe complexity ..."
flake8 --select C90 --max-complexity 10 --output-file "$OUTDIR/04_mccabe_complexity" $1

# pylint refactoring suggestions
echo "Step 5: pylint refactoring ..."
pylint -j 0 -d all -e R $1 > "$OUTDIR/05_pylint_refactoring"

# Type hints(AKA annotations) consistency
echo "Step 6: mypy type hints ..."
mypy --ignore-missing-imports $1 > "$OUTDIR/06_mypy_type_hints"

