#! /bin/bash

docker-compose -f "docker-compose.dev.yml" build soweego
docker-compose -f "docker-compose.dev.yml" run --rm soweego