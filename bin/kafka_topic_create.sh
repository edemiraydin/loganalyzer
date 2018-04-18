#!/bin/bash

#################################################################
#Kafka Topic creation                          #
#################################################################


topic_names=("log")
zookeeper=ahlclotxpla705.evv1.ah-isd.net:2181/kafka
replication_factor=2
partition=3

res=$(/bin/kafka-topics --list --zookeeper $zookeeper)
out=$(echo $res | tr " " "\n")
for new in "${topic_names[@]}"
   do
      kafka-topics --create --if-not-exists --zookeeper $zookeeper --replication-factor $replication_factor --partitions $partition --topic $new
   done