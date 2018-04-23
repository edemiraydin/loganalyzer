 /usr/bin/spark2-submit \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 \
--files /home/ejohn001/kafka_client_jaas.conf  \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/home/ejohn001/kafka_client_jaas.conf" \
--driver-java-options "-Djava.security.auth.login.config=/home/ejohn001/kafka_client_jaas.conf" \
--master local[2] \
--deploy-mode client \
--executor-memory 5g \
--executor-cores 10 \
--driver-memory 10g \
--conf spark.driver.maxResultSize=8g \
--conf spark.kryoserializer.buffer.max=512m \
./analyze.py localhost:9093 test ddos_output_rule1 log
