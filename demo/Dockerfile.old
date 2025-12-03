FROM cnfldemos/cp-server-connect-datagen:0.6.2-7.5.0
# 0.6.4-7.6.0
# FROM cnfldemos/cp-server-connect-datagen:0.6.7-8.0.0  <-- amd64 only

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components,/etc/kafka-connect/jars/"

USER root

RUN echo "===> Creating /app dir ..." \
     && mkdir -p /app \
     && chmod -R ag+w /app

RUN microdnf install -y gcc
RUN microdnf install -y gcc-c++
RUN microdnf install -y findutils
RUN microdnf install -y openssl-devel
RUN microdnf install -y bzip2-devel
RUN microdnf install -y libffi-devel
RUN microdnf install -y zlib-devel
RUN microdnf install -y which make
ENV JAVA_HOME=/usr/lib/jvm/zulu11

RUN cd /usr/src && \
    wget https://www.python.org/ftp/python/3.13.9/Python-3.13.9.tgz && \
    tar xzf Python-3.13.9.tgz && \
    cd Python-3.13.9 && \
    ./configure --enable-optimizations && \
    make altinstall && \
    ln -s /usr/local/bin/python3.13 /usr/bin/python3.13 && \
    ln -s /usr/local/bin/pip3.13 /usr/bin/pip3.13 && \
    cd /usr/src && \
    rm -rf Python-3.13.9.tgz Python-3.13.9

# Verify Python installation
RUN python3.13 --version

RUN python3.13 -m venv /tmp/venv313
RUN source /tmp/venv313/bin/activate
# RUN dnf install -y python-devel
RUN pip3.13 install pemja

USER appuser

COPY confluent-pytools-0.3.1-jar-with-dependencies.jar /etc/kafka-connect/jars/
COPY *.py /app/
COPY requirements.txt /app/
COPY *.json /app/
