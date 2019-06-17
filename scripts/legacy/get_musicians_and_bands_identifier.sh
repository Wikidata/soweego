#!/usr/bin/env bash

usage="Usage: $(basename "$0") IDENTIFIER_PROPERTY"
if [[ $# -ne 1 ]]; then
        echo $usage
        exit 1
fi

# Count QIDs that have the given identifier, with occupation = musician and all its direct subclasses
musicians=$(curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q639669 ; wdt:""$1"" ?identifier . }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total musicians:' $musicians
paging=$(($musicians/1000+1))
for i in $(seq 0 $paging); do curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item ?identifier WHERE { ?item wdt:P106/wdt:P279* wd:Q639669 ; wdt:""$1"" ?identifier . } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | sed 's/<http:\/\/www.wikidata.org\/entity\///' | sed 's/>//' >> musicians_with_${1}; done

# Count QIDs that are instance of band and all its direct subclasses
bands=$(curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P31/wdt:P279* wd:Q215380  ; wdt:""$1"" ?identifier . }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total bands:' $bands
paging=$(($bands/1000+1))
for i in $(seq 0 $paging); do curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item ?identifier WHERE { ?item wdt:P31/wdt:P279* wd:Q215380 ; wdt:""$1"" ?identifier . } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | sed 's/<http:\/\/www.wikidata.org\/entity\///' | sed 's/>//' >> bands_with_${1}; done
