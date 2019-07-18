cd /srv/prod/soweego/
/usr/bin/tmux kill-session -t imdb
/usr/bin/tmux new-session -d -s imdb
/usr/bin/tmux send-keys -t imdb:1 "./docker/pipeline.sh -c ../credentials.json -s /srv/prod/imdb/ imdb --no-upload --validator" ENTER
