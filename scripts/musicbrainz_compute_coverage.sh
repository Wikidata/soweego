#!/usr/bin/env bash


# Checks if the arguments are given
if [ -z "$1" ]
  then    
    echo 'Missing argument: Soweego project path'
    exit 1
fi

if [ -z "$2" ]
  then    
    echo 'Missing argument: Musicbrainz dump path'
    exit 1
fi

if [ -z "$3" ]
  then    
    echo 'Missing argument: url formatters'
    echo 'Run the following command to retrieve them.'
    echo 'pipenv run python3 -m soweego wikidata get_url_formatters_for_properties'
    exit 1
fi

if [ -z "$4" ]
  then    
    echo 'Missing argument: QID sample'
    exit 1
fi

if [ -z "$5" ]
  then    
    echo 'Missing argument: names sample'
    exit 1
fi

sample=$4
sample_labels=$5

# Creates a temp folder for auxiliary storage
tmp_path="$(pwd)/tmp_coverage_computation"
mkdir "$tmp_path"

# Matchers
(
    # Perfect label matches computation
    cd "$1"
    pipenv run python3 -m soweego target_selection musicbrainz get_label_musicbrainzid_dict "$2" -o "$tmp_path"
    pipenv run python3 -m soweego target_selection common perfect_strings_match "$sample_labels" "$tmp_path/artists.json" -o "$tmp_path"
    mv "$tmp_path/matches.json" "$tmp_path/label_matches.json"

    # Perfect link and sitelink match
    cd "$1"
    pipenv run python3 -m soweego wikidata get_links_for_sample "$sample_labels" "$3" -o "$tmp_path"
    pipenv run python3 -m soweego wikidata get_sitelinks_for_sample "$sample_labels" -o "$tmp_path"
    pipenv run python3 -m soweego target_selection musicbrainz links_match "$2" "$tmp_path/sample_links.json" "$tmp_path/sample_sitelinks.json" -o "$tmp_path"

    # Dates match
    pipenv run python3 -m soweego wikidata get_birth_death_dates_for_sample "$sample_labels" -o "$tmp_path"
    pipenv run python3 -m soweego target_selection musicbrainz get_users_label_dates_dictionary "$2" "$tmp_path/artists.json" -o "$tmp_path"
    pipenv run python3 -m soweego target_selection common perfect_strings_match "$tmp_path/sample_dates.json" "$tmp_path/labeldates_mbid.json" -o "$tmp_path"
    mv "$tmp_path/matches.json" "$tmp_path/dates_matches.json"
)
# Result evaluation
(
    cd "$tmp_path"
    count=$(wc -l label_matches.json link_match.json | grep total | grep -oP '\d+') 
    count=$(($count - 3))
    echo "Total matches = $count"
    unique=$(jq keys label_matches.json link_match.json | egrep -v '\[|\]' | sort -u | wc -l | grep -oP '\d+')
    echo "Unique matches = $unique"
	not_linked=$(wc -l ~/soweego/scripts/P434_musicians_sample | cut -d ' ' -f1)
	echo "WD items without MusicBrainz link = $not_linked"
    coverage_total=$(echo "$count / $not_linked" | bc -l)
    coverage_unique=$(echo "$unique / $not_linked" | bc -l)
    echo "Coverage estimation over total matches = $coverage_total"
    echo "Coverage estimation over unique matches = $coverage_unique"
)


# Deletes the temp folder for auxiliary storage
rm -rf "$tmp_path"
