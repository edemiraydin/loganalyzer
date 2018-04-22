from __future__ import print_function

import os
import sys
from pyspark import SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time
from datetime import datetime
import argparse
import json


#########################################
# 04/18/2018  E. Johnson    
# Spark Streaming code to stream ip,timestamp (from Apache logs)
# and analyze them for a potential DoS attack
# The rule to identify a possible DoS attack is checking for 5 or more attempts in one minute.
# Usage: analyze.py <zk> <checkpoint-directory> <output-file> <topic>
#  <zk> describe the list of Kafka Brokers
#   <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
#   <output-file> file to which the list of offending IPs are written
#   <topic>  name of the Kafka topic
#  example
#    `$ bin/spark-submit analyze.py \
#        ahlclotxpla706.evv1.ah-isd.net:2181 checkpoint out  log`
# If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
# a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
# checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
# the checkpoint data.
##########################################

if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("Usage: analyze.py "
             "<broker-list> <checkpoint-directory> <output-file> <topic>", file=sys.stderr)
		sys.exit(-1)
	
	
	sc = SparkContext(appName="LogAnalyzer")
	ssc = StreamingContext(sc, 2)
	ssc.checkpoint('checkpoint')
	 
	brokers, checkpoint, output, topic = sys.argv[1:]
	
	# Create Kafka DStream
	kvs=KafkaUtils.createStream(ssc, brokers, checkpoint, {topic: 1})
	#Get a DStream of JSON log entries
	parsed = kvs.map(lambda v: json.loads(v))	
	#Check counts of IPs by key
	r1_parsed= parsed.map(lambda log: (log['clientip'],1)).\
			reduceByKey(lambda x,y: x + y)
	
		
	# Count the number of distinct os values per ip
	r2_parsed= parsed.map(lambda a: (a['clientip']['os'],1)).\
			reduceByKey(lambda x,y: x + y)

	
	# Get a list of potential malicious IPs based on Rule 1
	bad_ips1 = r1_parsed.filter(lambda (k,v): v >= 10)
	bad_ips1.pprint()
 
	# Get a list of potential malicious IPs based on Rule 2
	bad_ips2 = r2_parsed.filter(lambda (k,v): v >= 10)
	bad_ips2.pprint()
	
	
	# Write results to HDFS 
	bad_ips1.saveAsTextFiles('ddos_output_rule1')
	bad_ips2.saveAsTextFiles('ddos_output_rule2')
	
	#Start the execution of the streams.
	ssc.start()
	# Wait for the execution to stop.
	ssc.awaitTermination()