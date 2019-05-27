cd /srv/dryrun/soweego/
/usr/bin/tmux kill-session -t pipeline-imdb
/usr/bin/tmux new-session -d -s "pipeline-imdb" ./scripts/docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/dryrun/shared/ imdb --no-upload --validator