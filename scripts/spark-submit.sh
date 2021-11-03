~/.spark/bin/spark-submit \
--files ./scripts/triplets.soda2103  \
--deploy-mode client --driver-memory 5g --executor-memory 6g --conf "spark.executor.memoryOverhead=10g" \
target/wQFMSpark-1.0-SNAPSHOT-jar-with-dependencies.jar