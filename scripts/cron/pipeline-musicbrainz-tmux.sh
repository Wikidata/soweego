cd /srv/dryrun/soweego/
/usr/bin/tmux kill-session -t pipeline-musicbrainz
/usr/bin/tmux new-session -d -s "pipeline-musicbrainz" ./scripts/docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/dryrun/musicbrainz-shared/ musicbrainz --no-upload --validator
/usr/bin/tmux set remain-on-exit on -t pipeline-musicbrainz
