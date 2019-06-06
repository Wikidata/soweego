cd /srv/dryrun/soweego/
/usr/bin/tmux kill-session -t pipeline-musicbrainz
/usr/bin/tmux new-session -d -s "pipeline-musicbrainz"
/usr/bin/tmux send-keys -t pipeline-musicbrainz:0 "./scripts/docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/dryrun/musicbrainz-shared/ musicbrainz --no-upload --validator" ENTER
