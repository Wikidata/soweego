cd /srv/soweego/
/usr/bin/tmux kill-session -t pipeline-discogs
/usr/bin/tmux new-session -d -s "pipeline-discogs"  
/usr/bin/tmux send-keys -t pipeline-discogs:0 "./scripts/docker/launch_pipeline.sh -c ../prod_cred.json -s /srv/discogs-shared/ discogs --no-upload --validator" ENTER
