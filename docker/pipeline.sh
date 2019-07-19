#!/usr/bin/env bash

PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-c FILE] [-s DIRECTORY] [ARGS]

-c FILE       Credentials file. Default: soweego/importer/resources/credentials.json
-s DIRECTORY  Output directory. Default: /tmp/soweego_shared/

ARGS are optional arguments passed to the soweego pipeline.

EOF
  exit 1
}

# Exit when no args are passed
if [ $# -eq 0 ]; then
    usage
fi

export DOCKER_SHARED_FOLDER="/tmp/soweego_shared/"
export CREDENTIALS_PATH="soweego/importer/resources/credentials.json"

while getopts :c:s: o; do
  case $o in
    (c) export CREDENTIALS_PATH="$OPTARG";;
    (s) export DOCKER_SHARED_FOLDER="$OPTARG";;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"


PARENT_FOLDER="$(dirname ${DOCKER_SHARED_FOLDER})"
FOLDER_NAME="$(basename ${DOCKER_SHARED_FOLDER})"
NUMBER_OF_BACKUPS=$(find ${PARENT_FOLDER} -maxdepth 1 -name "${FOLDER_NAME}*.tar.bz2" | wc -l)
NUMBER_OF_BACKUPS="${NUMBER_OF_BACKUPS// /}"
echo "${NUMBER_OF_BACKUPS} backups available"

# Remove the oldest backup
if [[ ${NUMBER_OF_BACKUPS} = "3" ]]; then
    to_rem=$(find ${PARENT_FOLDER} -maxdepth 1 -name "${FOLDER_NAME}*.tar.bz2" | sort | head -n 1)
    echo "Deleting older backup: ${to_rem}"
    rm -f ${to_rem}
fi

# Back up and reset the Docker shared folder
(
cd "${PARENT_FOLDER}"
# Restore permissions due to Docker writing files as root
sudo chown -R hjfocs:wikidev ${FOLDER_NAME}
NOW=$(date +"%Y_%m_%d_%H_%M")
tar -cvjf "${FOLDER_NAME}_${NOW}.tar.bz2" ${FOLDER_NAME}
rm -rf ${FOLDER_NAME}
mkdir -p ${FOLDER_NAME}
)

# Reset and update the project source code
git reset --hard HEAD
git clean -f -d
git pull

# Set up the credentials file
if [[ -f "$CREDENTIALS_PATH" ]]; then
    cp "${CREDENTIALS_PATH}" "${DOCKER_SHARED_FOLDER}/credentials.json"
fi

# Build and run Docker
docker build --rm -f "Dockerfile.pipeline" -t maxfrax/soweego:pipeline .
docker run -it --rm --name soweego-pipeline-$RANDOM --volume "${DOCKER_SHARED_FOLDER}":"/app/shared" maxfrax/soweego:pipeline "$@"

