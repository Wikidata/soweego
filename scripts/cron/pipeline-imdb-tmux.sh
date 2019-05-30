cd /srv/dryrun/soweego/
/usr/bin/tmux kill-session -t pipeline-imdb
/usr/bin/tmux new-session -d -s "pipeline-imdb" ./scripts/docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/dryrun/imdb-shared/ imdb --no-upload --validator
/usr/bin/tmux set remain-on-exit on -t pipeline-imdb
