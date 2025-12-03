FROM python:3.13

USER root

RUN echo "===> Creating /app dir ..." \
     && mkdir -p /app \
     && chmod -R ag+w /app

RUN useradd -ms /bin/bash appuser

#--------------> install java + Connect

RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless lsb-release iputils-ping nano && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

RUN ln -s $(dirname $(dirname `readlink -f /etc/alternatives/java`)) /etc/alternatives/java_home
ENV JAVA_HOME=/etc/alternatives/java_home

RUN mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.confluent.io/deb/8.1/archive.key | gpg \
    --dearmor | tee /etc/apt/keyrings/confluent.gpg > /dev/null

RUN echo "Types: deb \n\
URIs: https://packages.confluent.io/deb/8.1 \n\
Suites: bookworm \n\
Components: main \n\
Architectures: $(dpkg --print-architecture) \n\
Signed-by: /etc/apt/keyrings/confluent.gpg \n\
" | tee /etc/apt/sources.list.d/confluent-platform.sources > /dev/null

# removed: $(lsb_release -cs) from client/Suites line
# removed: clients source

# RUN apt-get update && apt-get install confluent-platform

COPY confluent-kafka_8.1.0-1_all.deb .
RUN apt install ./confluent-kafka_8.1.0-1_all.deb

#--------------> python env

ENV PIP_DEFAULT_TIMEOUT=100 \
    # Allow statements and log messages to immediately appear
    PYTHONUNBUFFERED=1 \
    # disable a pip version check to reduce run-time & log-spam
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # cache is useless in docker image, so disable to reduce image size
    PIP_NO_CACHE_DIR=1

RUN python3 -m ensurepip --upgrade \
  && pip3 install --no-cache --upgrade pip setuptools wheel virtualenv

ENV VIRTUAL_ENV=/tmp/venv313
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt
#COPY pemja-0.7.dev0.tar.gz /app/
#RUN pip install -vv /app/pemja-0.7.dev0.tar.gz

USER appuser

# /usr/share/java default in connect properties
COPY confluent-pytools-0.3.1-jar-with-dependencies.jar /usr/share/java

COPY *.py /app/
COPY *.json /app/
COPY connector.properties.tpl /app/
COPY start-connect.sh /app/

USER root

###########################################################
# 3 ways to provide Kafka Connect properties to the worker:
###########################################################
# 1: provide env vars below or with the '-e' flag when calling 'docker run'
# 2: or use a mix of ENV VARS below and properties that you add to connector.properties.tpl
# format for env vars = (1) prefix with CONNECT_ (2) use _ to separate elements
# --> CONNECT_BOOTSTRAP_SERVERS becomes bootstrap.servers

# ENV CONNECT_BOOTSTRAP_SERVERS=localhost:29092

# 3: provide your own file and uncomment the following line
# COPY connect.properties /etc/kafka/connect.properties

CMD ["sleep", "infinity"]
# for option 1 and 2, you have to use (it includes putting the env vars into the properties file):
#CMD ["/app/start-connect.sh"]
# for option 3, you can use:
# CMD ["/usr/bin/connect-distributed", "/etc/kafka/connect.properties"]