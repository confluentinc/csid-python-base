#!/bin/sh
python /app/env_to_props.py /app/connector.properties.tpl  /etc/kafka/connect.properties
/usr/bin/connect-distributed /etc/kafka/connect.properties