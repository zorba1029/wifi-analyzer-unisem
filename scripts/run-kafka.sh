#!/usr/bin/env bash
#!/bin/bash
nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties > /dev/null 2>&1 &

#--- create topic test-topic-collector
# /opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic "test-topic-collector"

#--- create topic test-topic-analyzer
# /opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic "test-topic-analyzer"
