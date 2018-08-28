#!/usr/bin/env bash

# Count QIDs with occupation = musician and all its direct subclasses
musicians=$(curl -G -H 'Accept:text/tab-separated-values' --data-urlencode 'query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P106/wdt:P279* wd:Q639669 . }' https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total musicians:' $musicians
paging=$(($musicians/1000+1))
for i in $(seq 0 $paging); do curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P106/wdt:P279* wd:Q639669 . } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> musicians; done

# Count QIDs that are instance of band and all its direct subclasses
bands=$(curl -G -H 'Accept:text/tab-separated-values' --data-urlencode 'query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P31/wdt:P279* wd:Q215380 . }' https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
echo 'Total bands:' $bands
paging=$(($bands/1000+1))
for i in $(seq 0 $paging); do curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P31/wdt:P279* wd:Q215380 . } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> bands; done

musicians_sample=$(($musicians/100))
echo 'Dumping musicians sample, size:' $musicians_sample
shuf -n $musicians_sample musicians >> musicians_and_bands_1_percent_sample
bands_sample=$(($bands/100))
echo 'Dumping bands sample, size:' $bands_sample
shuf -n $bands_sample bands >> musicians_and_bands_1_percent_sample

