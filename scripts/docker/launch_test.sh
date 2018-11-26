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

while getopts :s: o; do
  case $o in
    (s) export DOCKER_SHARED_FOLDER="$OPTARG";;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

docker-compose -f "docker-compose.dev.yml" up -d --build
docker-compose -f "docker-compose.dev.yml" exec soweego /bin/bash