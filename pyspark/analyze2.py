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
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SQLContext

#########################################
# 04/18/2018  E. Johnson    
# Spark Streaming code to stream ip,timestamp (from Apache logs)
# and analyze them for a potential DoS attack. The unique IP, os entries will be upserted into a Kudu table
# THIS CODE IS IN PROGRESS 
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


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sc)
    return globals()['sqlContextSingletonInstance']
	
#Upsert distinct IP, OS pairs (30 sec) into a Kudu table 
def insert_into_kudu(time,rdd):
    sqc = getSqlContextInstance(rdd.context)
    kudu_df = rdd.toDF(['ip','os']).dropDuplicates()
	#kudu_df.write.format('org.apache.kudu.spark.kudu').option('kudu.master',"ip:7051").option('kudu.table',"ip_counts_last_30").mode("append").save()

	
if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("Usage: analyze.py "
             "<broker-list> <checkpoint-directory> <output-file> <topic> ", file=sys.stderr)
		sys.exit(-1)
	
	brokers, checkpoint, output, topic = sys.argv[1:]
	
	
	sc = SparkContext(appName="LogAnalyzer")	

	ssc = StreamingContext(sc, 30)
	ssc.checkpoint('checkpoint')
	 
 	# Create Kafka DStream
	kvs=KafkaUtils.createStream(ssc, brokers, checkpoint, {topic: 1})

	#Get a DStream of JSON log entries
	parsed = kvs.map(lambda v: json.loads(v))	
	 
		
	# Count the number of distinct os values per ip
	r2_parsed= parsed.map(lambda a: (str(a['clientip']), str(a['os'])))

	r2_parsed.foreachRDD(insert_into_kudu)
	 
		
	
	#Start the execution of the streams.
	ssc.start()
	# Wait for the execution to stop.
	ssc.awaitTermination()
