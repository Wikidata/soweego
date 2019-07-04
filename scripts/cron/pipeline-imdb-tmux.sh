cd /srv/soweego_production/soweego/
/usr/bin/tmux kill-session -t pipeline-imdb
/usr/bin/tmux new-session -d -s "pipeline-imdb"
/usr/bin/tmux send-keys -t pipeline-imdb:0 "./docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/soweego_production/imdb-shared/ imdb --no-upload --validator" ENTER
