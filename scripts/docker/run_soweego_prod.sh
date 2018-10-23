#! /bin/bash

old_credentials="soweego/importer/resources/db_credentials.json.old"

source .env

# Checks if the arguments are given
if [ -z "$1" ]
  then
    echo "Running soweego with test database credentials"
    if [ -f "$old_credentials" ]
    then
        mv soweego/importer/resources/db_credentials.json.old soweego/importer/resources/db_credentials.json
    fi
    docker build --rm -f "Dockerfile" -t maxfrax/soweego:latest .
    docker run -it --rm --name soweego --env-file .env --volume "${DUMP_FOLDER}":"/app/dump" maxfrax/soweego:latest /bin/bash
    exit 0
fi

echo "Running soweego with given database credentials"
if [ ! -f "$old_credentials" ]
then
    mv soweego/importer/resources/db_credentials.json $old_credentials
fi
cp $1 soweego/importer/resources/db_credentials.json
docker build --rm -f "Dockerfile" -t maxfrax/soweego:latest .
docker run -it --rm --name soweego --env-file .env --volume "${DUMP_FOLDER}":"/app/dump" maxfrax/soweego:latest /bin/bash