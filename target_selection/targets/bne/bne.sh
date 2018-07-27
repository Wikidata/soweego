#!/usr/bin/env bash

# BNE ontology: http://datos.bne.es/def/
# C1005 = people class
# P5001 = name property
# P5012 = also known as property
# Page everything in buckets of 1k results

# Names: 1,269,330
for i in {0..1270}; do curl -G -H 'Accept:text/csv' --data-urlencode "query=prefix bne: <http://datos.bne.es/def/> select ?id ?name where { ?id a bne:C1005 ; bne:P5001 ?name . } OFFSET ""$((i*1000))"" LIMIT 1000" http://datos.bne.es/sparql | tail -n +2 >> people; done
# AKA: 342,299
for i in {0..343}; do curl -G -H 'Accept:text/csv' --data-urlencode "query=prefix bne: <http://datos.bne.es/def/> select ?person ?aka where { ?person a bne:C1005 ; bne:P5012 ?aka . } OFFSET ""$((i*1000))"" LIMIT 1000" http://datos.bne.es/sparql | tail -n +2 >> aka_people; done
# sameAs links: 1,626,703
for i in {0..1627}; do curl -G -H 'Accept:text/csv' --data-urlencode "query=prefix bne: <http://datos.bne.es/def/> select ?person ?link where { ?person a bne:C1005 ; owl:sameAs ?link . } OFFSET ""$((i*1000))"" LIMIT 1000" http://datos.bne.es/sparql | tail -n +2 >> linked_people; done

