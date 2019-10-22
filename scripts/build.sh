#!/bin/bash
# To be called with scripts/build.sh
set -ex
# SET THE FOLLOWING VARIABLES
# docker hub username
USERNAME=pueteam
# image name
IMAGE=kudujs
docker build -t $USERNAME/$IMAGE:latest .
