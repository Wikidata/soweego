#!/usr/bin/env bash

cd ~/bne
count=$(wc -l names_perfect_matches.json aka_perfect_matches.json link_matches.json enwiki_matches.json name_and_date_matches.json | grep total | cut -d ' ' -f2)
count=$(($count - 5))
echo "Total matches = $count"
unique=$(jq keys names_perfect_matches.json aka_perfect_matches.json link_matches.json enwiki_matches.json name_and_date_matches.json | egrep -v '\[|\]'| sort -u | wc -l | cut -d ' ' -f1)
echo "Unique matches = $unique"
not_linked=$(wc -l ~/wikidata/bne_humans_not_linked | cut -d ' ' -f1)
echo "WD items without BNE link = $not_linked"
coverage_total=$(echo "$count / $not_linked" | bc -l)
coverage_unique=$(echo "$unique / $not_linked" | bc -l)
echo "Coverage estimation over total matches = $coverage_total"
echo "Coverage estimation over unique matches = $coverage_unique"

