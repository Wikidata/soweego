#!/usr/bin/env bash

usage="Usage: $(basename "$0") CATALOG_PROPERTY_ID"
if [[ $# -ne 1 ]]; then
        echo $usage
        exit 1
fi

# Count QIDs with occupation = actor and all its direct subclasses
echo "Counting actors with no "$1" link ..."
actors=$(curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q33999 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total unlinked actors:' $actors
paging=$(($actors/1000+1))

# Run paged query
echo "About to run $paging paged queries ..."
for i in $(seq 0 $paging); do curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P106/wdt:P279* wd:Q33999 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> "$1"_unlinked_actors; echo $i; done
sort -u "$1"_unlinked_actors > "$1"_unique_sorted_unlinked_actors

# Count QIDs with occupation = producer and all its direct subclasses
echo "Counting producers with no "$1" link ..."
producers=$(curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q47541952 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total unlinked producers:' $producers
paging=$(($producers/1000+1))

# Run paged query
echo "About to run $paging paged queries ..."
for i in $(seq 0 $paging); do curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P106/wdt:P279* wd:Q47541952 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> "$1"_unlinked_producers; echo $i; done
sort -u "$1"_unlinked_producers > "$1"_unique_sorted_unlinked_producers

echo "Unlinked actors from SPARQL: ""$(wc -l "$1"_unlinked_actors)"""
unique_actors=$(wc -l "$1"_unique_sorted_unlinked_actors | cut -d ' ' -f1)
echo "Actual unique actors: ""$unique_actors"""

echo "Unlinked producers from SPARQL: ""$(wc -l "$1"_unlinked_producers)"""
unique_producers=$(wc -l "$1"_unique_sorted_unlinked_producers | cut -d ' ' -f1)
echo "Actual unique producers: ""$unique_producers"""

actors_sample=$(($unique_actors/100))
echo 'Dumping actors sample, size:' $actors_sample
shuf -n $actors_sample "$1"_unique_sorted_unlinked_actors > "$1"_actors_sample
producers_sample=$(($unique_producers/100))
echo 'Dumping producers sample, size:' $producers_sample
shuf -n $producers_sample "$1"_unique_sorted_unlinked_producers > "$1"_producers_sample

