docker login
docker build . -t connect-with-devtools:1.1.0 -t laubory/connect-with-devtools:latest
docker image push laubory/connect-with-devtools:latest