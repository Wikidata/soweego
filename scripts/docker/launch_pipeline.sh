#!/bin/bash

source .env

PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-s <dir>][-c <file>] 

-s <dir>: ...
-c <file>: ...

EOF
  exit 1
}

export DOCKER_SHARED_FOLDER="/tmp/soweego_shared/"
export CREDENTIALS_PATH="soweego/importer/resources/db_credentials.json"

while getopts :s:c: o; do
  case $o in
    (s) export DOCKER_SHARED_FOLDER="$OPTARG";;
    (c) export CREDENTIALS_PATH="$OPTARG";;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

cp "$CREDENTIALS_PATH" "$DOCKER_SHARED_FOLDER/credentials.json"

docker build --rm -f "Dockerfile.pipeline" -t maxfrax/soweego:pipeline .
docker run -it --rm --name soweego-pipeline --env-file .env --volume "${DOCKER_SHARED_FOLDER}":"/app/shared" maxfrax/soweego:pipeline -c "/app/shared/credentials.json" "$@"