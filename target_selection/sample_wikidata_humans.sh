#!/usr/bin/env bash

# Count QIDs that are instance of humans and all its direct subclasses
# WARNING: the query may time out. Result retrieved on 25/07/2018 = 4409859
humans=$(curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT (COUNT(DISTINCT ?item) AS ?count) WHERE { ?item wdt:P31/wdt:P279* wd:Q5 . }" https://query.wikidata.org/sparql | tail -n +2 | cut -d '"' -f2)
humans=4409859
paging=$(($humans/1000+1))
paging=1
for i in $(seq 0 $paging); do curl -G -H 'Accept:text/tab-separated-values' --data-urlencode "query=SELECT DISTINCT ?item WHERE { ?item wdt:P31/wdt:P279* wd:Q5 . } OFFSET ""$((i*1000))"" LIMIT 1000" https://query.wikidata.org/sparql | cut -f2 | grep -oP 'Q\d+' >> humans; done

sample=$(($humans/100))
shuf -n $sample humans > humans_1_percent_sample 

