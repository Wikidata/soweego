#!/usr/bin/env python3
# coding: utf-8

from collections import defaultdict

def equal_strings_match(databases) -> dict:
    """Given a touple of dictionaries (string-id), returns the id-id matches if any"""
    # Baseline matcher 1: perfect strings
    # Perfect matches against BNE names
    matched = defaultdict(list)
    for d in databases:
        for k, v in d.items():
            matched[k].append(v)
    return {v[0]: v[1] for v in matched.values() if len(v) > 1}
