#!/bin/sh
if [ $# -eq 0 ]
  then
    echo "Please pass the version number as the first (and only) argument"
    exit 1
fi

docker login
docker build . -t connect-with-devtools:$1  -t europe-west9-docker.pkg.dev/csid-281116/pytools/connect-python:$1 --platform=linux/amd64
docker push europe-west9-docker.pkg.dev/csid-281116/pytools/connect-python:$1
