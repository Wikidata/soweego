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

# Count QIDs with occupation = teacher and all its direct subclasses
echo "Counting teachers with no "$1" link ..."
teachers=$(curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q37226 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total unlinked teachers:' $teachers
paging=$(($teachers/1000+1))

# Run paged query
echo "About to run $paging paged queries ..."
for i in $(seq 0 $paging); do curl -s -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P106/wdt:P279* wd:Q37226 . FILTER NOT EXISTS { ?item wdt:""$1"" ?identifier . } } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> "$1"_unlinked_teachers; echo $i; done
sort -u "$1"_unlinked_teachers > "$1"_unique_sorted_unlinked_teachers

echo "Unlinked teachers from SPARQL: ""$(wc -l "$1"_unlinked_teachers)"""
unique_teachers=$(wc -l "$1"_unique_sorted_unlinked_teachers | cut -d ' ' -f1)
echo "Actual unique teachers: ""$unique_teachers"""

authors_sample=$(($unique_authors/100))
echo 'Dumping authors sample, size:' $authors_sample
shuf -n $authors_sample "$1"_unique_sorted_unlinked_authors > "$1"_authors_sample
teachers_sample=$(($unique_teachers/100))
echo 'Dumping teachers sample, size:' $teachers_sample
shuf -n $teachers_sample "$1"_unique_sorted_unlinked_teachers > "$1"_teachers_sample

