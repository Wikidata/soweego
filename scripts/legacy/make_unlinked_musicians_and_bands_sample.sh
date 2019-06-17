#!/usr/bin/env bash

usage="Usage: $(basename "$0") CATALOG_PROPERTY_ID"
if [[ $# -ne 1 ]]; then
        echo $usage
        exit 1
fi

# Count QIDs with occupation = musician and all its direct subclasses
echo "Counting musicians with no "$1" link ..."
musicians=$(curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q639669 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total unlinked musicians:' $musicians
paging=$(($musicians/1000+1))

# Run paged query
echo "About to run $paging paged queries ..."
for i in $(seq 0 $paging); do curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P106/wdt:P279* wd:Q639669 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> "$1"_unlinked_musicians; echo $i; done
sort -u "$1"_unlinked_musicians > "$1"_unique_sorted_unlinked_musicians

echo "Unlinked musicians from SPARQL: ""$(wc -l "$1"_unlinked_musicians)"""
unique_musicians=$(wc -l "$1"_unique_sorted_unlinked_musicians | cut -d ' ' -f1)
echo "Actual unique musicians: ""$unique_musicians"""

# Count QIDs that are instance of band and all its direct subclasses
echo "Counting bands with no "$1" link ..."
bands=$(curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P31/wdt:P279* wd:Q215380 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total unlinked bands:' $bands
paging=$(($bands/1000+1))

# Run paged query
echo "About to run $paging paged queries ..."
for i in $(seq 0 $paging); do curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P31/wdt:P279* wd:Q215380 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> "$1"_unlinked_bands; echo $i; done
sort -u "$1"_unlinked_bands > "$1"_unique_sorted_unlinked_bands

echo "Unlinked bands from SPARQL: ""$(wc -l "$1"_unlinked_bands)"""
unique_bands=$(wc -l "$1"_unique_sorted_unlinked_bands | cut -d ' ' -f1)
echo "Actual unique bands: ""$unique_bands"""

musicians_sample=$(($unique_musicians/100))
echo 'Dumping musicians sample, size:' $musicians_sample
shuf -n $musicians_sample "$1"_unique_sorted_unlinked_musicians > "$1"_musicians_sample
bands_sample=$(($unique_bands/100))
echo 'Dumping bands sample, size:' $bands_sample
shuf -n $bands_sample "$1"_unique_sorted_unlinked_bands > "$1"_bands_sample

