FROM confluentinc/cp-kafka-connect:7.3.4

USER root

RUN echo "===> Creating /app dir ..." \
     && mkdir -p /app \
     && chmod -R ag+w /app

RUN microdnf install gcc-c++ gcc findutils
RUN microdnf install which
ENV JAVA_HOME=/usr/lib/jvm/zulu11

RUN python3.9 -m venv /tmp/venv39
RUN source /tmp/venv39/bin/activate
RUN dnf install -y python39-devel
RUN pip3 install pemja
