docker login
docker build . -t connect-with-devtools:1.0.11 -t ldom/connect-with-devtools:latest
docker image push ldom/connect-with-devtools:latest