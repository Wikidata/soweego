#!/usr/bin/env bash

cd ~/discogs
count=$(wc -l musicians_labels_perfect_matches.json musicians_links_perfect_matches.json musicians_wikilinks_perfect_matches.json | grep total | cut -d ' ' -f2)
count=$(($count - 5))
echo "Total matches = $count"
unique=$(jq keys musicians_labels_perfect_matches.json musicians_links_perfect_matches.json musicians_wikilinks_perfect_matches.json | egrep -v '\[|\]'| sort -u | wc -l | cut -d ' ' -f1)
echo "Unique matches = $unique"
not_linked=$(wc -l ~/wikidata/discogs_sample_not_linked | cut -d ' ' -f1)
echo "WD items without Discogs link = $not_linked"
coverage_total=$(echo "$count / $not_linked" | bc -l)
coverage_unique=$(echo "$unique / $not_linked" | bc -l)
echo "Coverage estimation over total matches = $coverage_total"
echo "Coverage estimation over unique matches = $coverage_unique"
