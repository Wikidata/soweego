cd /srv/prod/soweego/
/usr/bin/tmux kill-session -t discogs
/usr/bin/tmux new-session -d -s discogs
/usr/bin/tmux send-keys -t discogs:1 "./docker/pipeline.sh -c ../credentials.json -s /srv/prod/discogs/ discogs --no-upload --validator" ENTER
