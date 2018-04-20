from __future__ import print_function

import os
import sys
from pyspark import SparkContext 
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import time
from datetime import datetime
import argparse


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


def update_frequency(new_entry, freq_sum):
  if not freq_sum:
    freq_sum = 0
  return sum(new_entry) + freq_sum


if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("Usage: analyze.py "
             "<broker-list> <checkpoint-directory> <output-file> <topic>", file=sys.stderr)
		sys.exit(-1)
	start=datetime.now()
	sc = SparkContext(appName="LogAnalyzer")
	ssc = StreamingContext(sc, 2)
	ssc.checkpoint('checkpoint')
	
	brokers, checkpoint, output, topic = sys.argv[1:]
	kvs=KafkaUtils.createStream(ssc, 'ahlclotxpla706.evv1.ah-isd.net:2181,ahlclotxpla705.evv1.ah-isd.net:2181,ahlclotxpla707.evv1.ah-isd.net:2181/kafka', 'test6', {topic: 1})
	kvs.pprint()

	# parsed = kvs.map(lambda x: x[1])
	# lines = kvs.map(lambda x: x[1])
	# counts = lines.flatMap(lambda line: line.split(",")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
	# counts.pprint()
	# updated = parsed.updateStateByKey(update_frequency)
	# updated2 = updated.map(lambda (k,v): (str(k), v))
	# #high_freq = updated2.filter(lambda (k,v): v >= 85)
	# updated2.pprint()
	#high_freq.saveAsTextFiles('DDOS_attacker_found_output')
	ssc.start()
	#time.sleep(60)
	#ssc.stop()
	#ssc.awaitTermination()
	#print('Process ended.')
	#print('Time taken:', datetime.now()-start)
	#countStream = logStream.countByWindow(5,2)
   # countStream.pprint()
	#counts = lines.flatMap(lambda line: line.split(",")) \
    #   .map(lambda word: (word, 1)) \
    #    .reduceByKey(lambda a, b: a+b)
	#counts.pprint()
	#ssc.start()
	#ssc.awaitTermination()