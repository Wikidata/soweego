#!/bin/bash
PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-s <dir>]

-s <dir>: ...

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

if [[ -f "$CREDENTIALS_PATH" ]]; then
    cp "${CREDENTIALS_PATH}" "${DOCKER_SHARED_FOLDER}/credentials.json"
fi

docker-compose -f "docker-compose.dev.yml" up -d --build
docker-compose -f "docker-compose.dev.yml" exec soweego /bin/bash