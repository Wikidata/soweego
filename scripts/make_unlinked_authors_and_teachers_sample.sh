#!/usr/bin/env bash

usage="Usage: $(basename "$0") CATALOG_PROPERTY_ID"
if [[ $# -ne 1 ]]; then
        echo $usage
        exit 1
fi

# Count QIDs with occupation = author and all its direct subclasses
echo "Counting authors with no "$1" link ..."
authors=$(curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q482980 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total unlinked authors:' $authors
paging=$(($authors/1000+1))

# Run paged query
echo "About to run $paging paged queries ..."
for i in $(seq 0 $paging); do curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P106/wdt:P279* wd:Q482980 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> "$1"_unlinked_authors; echo $i; done
sort -u "$1"_unlinked_authors > "$1"_unique_sorted_unlinked_authors

echo "Unlinked authors from SPARQL: ""$(wc -l "$1"_unlinked_authors)"""
unique_authors=$(wc -l "$1"_unique_sorted_unlinked_authors | cut -d ' ' -f1)
echo "Actual unique authors: ""$unique_authors"""

authors_sample=$(($unique_authors/100))
echo 'Dumping authors sample, size:' $authors_sample
shuf -n $authors_sample "$1"_unique_sorted_unlinked_authors > "$1"_authors_sample
bands_sample=$(($unique_bands/100))
echo 'Dumping bands sample, size:' $bands_sample
shuf -n $bands_sample "$1"_unique_sorted_unlinked_bands > "$1"_bands_sample

