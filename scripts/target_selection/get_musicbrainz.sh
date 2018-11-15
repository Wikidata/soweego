#!/bin/bash

#For mac users
function sha256sum() { shasum -a 256 -c "$@" ; } && export -f sha256sum

gpg --recv-keys C777580F
latest_version=$(curl http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST)
working_dir_path="./musicbrainz_dump_$latest_version"
latest_version_web_folder="http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/$latest_version"
mkdir $working_dir_path
cd $working_dir_path
curl "$latest_version_web_folder/SHA256SUMS" --output "SHA256SUMS"
curl "$latest_version_web_folder/SHA256SUMS.asc" --output "SHA256SUMS.asc"
gpg --verify SHA256SUMS.asc SHA256SUMS
curl "$latest_version_web_folder/mbdump.tar.bz2" --output "mbdump.tar.bz2"
curl "$latest_version_web_folder/mbdump-derived.tar.bz2" --output "mbdump-derived.tar.bz2"
sha256sum SHA256SUMS
tar jvxf mbdump.tar.bz2
tar jvxf mbdump-derived.tar.bz2