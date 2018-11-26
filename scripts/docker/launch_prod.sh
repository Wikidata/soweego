#!/bin/bash

source .env

credentials_path="soweego/importer/resources/db_credentials.json"

PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-s <dir>] [-c <file>] 

-s <dir>: ...
-f <file>: ...

EOF
  exit 1
}

export DOCKER_SHARED_FOLDER="/tmp/soweego_shared/"
git checkout $credentials_path

while getopts :s:c: o; do
  case $o in
    (s) export DOCKER_SHARED_FOLDER="$OPTARG";;
    (c) cp $OPTARG $credentials_path;;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"


docker build --rm -f "Dockerfile.prod" -t maxfrax/soweego:latest .
docker run -it --rm --name soweego --env-file .env --volume "${DOCKER_SHARED_FOLDER}":"/app/shared" maxfrax/soweego:latest /bin/bash