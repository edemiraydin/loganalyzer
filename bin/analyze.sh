
/usr/bin/spark2-submit \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.2 \
--files /opt/cloudera/security/kafka_jaas.conf  \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/opt/cloudera/security/kafka_jaas.conf" \
--driver-java-options "-Djava.security.auth.login.config=/opt/cloudera/security/kafka_jaas.conf" \
--master local \
--deploy-mode client \
--executor-memory 35g \
--executor-cores 10 \
--driver-memory 20g \
--conf spark.driver.maxResultSize=8g \
--queue etl \
--conf spark.kryoserializer.buffer.max=512m \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.yarn.executor.memoryOverhead=8g \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
./analyze1.py ahlclotxpla705.evv1.ah-isd.net:9093 test test log


-files /etc/kafka/conf/kafka_jaas.conf,/etc/security/keytabs/kafka.service.keytab --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/etc/kafka/conf/kafka_jaas.conf" --driver-java-options "-Djava.security.auth.login.config=/etc/kafka/conf/kafka_jaas.conf" 

 /usr/bin/spark2-submit \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 \
--files /home/ejohn001/kafka_client_jaas.conf  \
--conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/home/ejohn001/kafka_client_jaas.conf" \
--driver-java-options "-Djava.security.auth.login.config=/home/ejohn001/kafka_client_jaas.conf" \
--master yarn \
--deploy-mode client \
--executor-memory 35g \
--executor-cores 10 \
--driver-memory 20g \
--conf spark.driver.maxResultSize=8g \
--queue etl \
--conf spark.kryoserializer.buffer.max=512m \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.yarn.executor.memoryOverhead=8g \
--conf spark.dynamicAllocation.maxExecutors=50 \
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
./analyze1.py ahlclotxpla705.evv1.ah-isd.net:9093 test test logs


--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.2 \
