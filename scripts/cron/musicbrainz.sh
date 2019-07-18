cd /srv/prod/soweego/
/usr/bin/tmux kill-session -t musicbrainz
/usr/bin/tmux new-session -d -s musicbrainz
/usr/bin/tmux send-keys -t musicbrainz:1 "./docker/pipeline.sh -c ../credentials.json -s /srv/prod/musicbrainz/ musicbrainz --no-upload --validator" ENTER
