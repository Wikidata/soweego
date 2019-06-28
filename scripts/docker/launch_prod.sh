#!/bin/bash

source .env

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

while getopts :s:c: o; do
  case $o in
    (s) export DOCKER_SHARED_FOLDER="$OPTARG";;
    (c) export CREDENTIALS_PATH="$OPTARG";;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

cp "${CREDENTIALS_PATH}" "${DOCKER_SHARED_FOLDER}/credentials.json"


docker build --rm -f "Dockerfile.test" -t maxfrax/soweego:latest .
docker run -it --rm --name soweego-prod-$RANDOM --env-file .env --volume "${DOCKER_SHARED_FOLDER}":"/app/shared" --volume "$(pwd)":"/app/soweego" maxfrax/soweego:latest /bin/bash
