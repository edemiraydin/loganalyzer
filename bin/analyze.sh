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
--queue etl \
--conf spark.kryoserializer.buffer.max=512m \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.yarn.executor.memoryOverhead=8g \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
./analyze.py localhost:9093 test test log