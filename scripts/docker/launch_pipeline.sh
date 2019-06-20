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
export CREDENTIALS_PATH="soweego/importer/resources/credentials.json"

while getopts :s:c: o; do
  case $o in
    (s) export DOCKER_SHARED_FOLDER="$OPTARG";;
    (c) export CREDENTIALS_PATH="$OPTARG";;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"


# Removes oldest backup
PARENT_FOLDER="$(dirname ${DOCKER_SHARED_FOLDER})"
FOLDER_NAME="$(basename ${DOCKER_SHARED_FOLDER})"
NUMBER_OF_BACKUPS=$(find ${PARENT_FOLDER} -maxdepth 1 -name "${FOLDER_NAME}*.tar.gz" | wc -l)
NUMBER_OF_BACKUPS="${NUMBER_OF_BACKUPS// /}"
echo "${NUMBER_OF_BACKUPS} backups available"
if [[ ${NUMBER_OF_BACKUPS} = "4" ]]; then
    to_rem=$(find ${PARENT_FOLDER} -maxdepth 1 -name "${FOLDER_NAME}*.tar.gz" | sort | head -n 1)
    echo "Deleting older backup: ${to_rem}"
    rm -f ${to_rem}
fi

# Creates a backup and resets the docker shared folder
NOW=$(date +"%Y_%m_%d_%H_%M")
tar -czvf "${DOCKER_SHARED_FOLDER}_${NOW}.tar.gz" ${DOCKER_SHARED_FOLDER}
rm -rf ${DOCKER_SHARED_FOLDER}
mkdir -p ${DOCKER_SHARED_FOLDER}

# Reset and update the project source code
git reset --hard HEAD
git clean -f -d
git pull

# Sets up the credentials file
cp "${CREDENTIALS_PATH}" "${DOCKER_SHARED_FOLDER}/credentials.json"

# Builds and runs docker
docker build --rm -f "Dockerfile.pipeline" -t maxfrax/soweego:pipeline .
docker run -it --rm --name soweego-pipeline-$RANDOM --env-file .env --volume "${DOCKER_SHARED_FOLDER}":"/app/shared" maxfrax/soweego:pipeline -c "/app/shared/credentials.json" "$@"
