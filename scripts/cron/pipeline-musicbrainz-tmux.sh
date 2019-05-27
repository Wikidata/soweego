cd /srv/dryrun/soweego/
/usr/bin/tmux kill-session -t pipeline-musicbrainz
/usr/bin/tmux new-session -d -s "pipeline-musicbrainz" ./scripts/docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/dryrun/shared/ musicbrainz --no-upload --validator
