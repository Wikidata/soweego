#! /bin/bash

# Put here the location of your dump work dir
export DUMP_FOLDER="/Volumes/Dati/soweego_files"

docker-compose -f "docker-compose.dev.yml" up -d --build
docker-compose -f "docker-compose.dev.yml" exec soweego /bin/bash