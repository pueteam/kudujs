#!/bin/bash

CONTAINER_NAME=kudujs_DEV

docker stop ${CONTAINER_NAME}
docker rm ${CONTAINER_NAME}

docker run -d --name ${CONTAINER_NAME} -v $(pwd):/app -v $(pwd)/src:/home/cdsw/src docker.repository.cloudera.com/cdsw/engine:8 /app/scripts/dev_entrypoint.sh