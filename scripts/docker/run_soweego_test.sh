#! /bin/bash

docker-compose -f "docker-compose.dev.yml" up -d --build
docker-compose -f "docker-compose.dev.yml" exec soweego /bin/bash