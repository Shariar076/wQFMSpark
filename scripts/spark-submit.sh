/dfs/.spark/bin/spark-submit \
  --conf "spark.executor.memory=20g" \
  --conf "spark.driver.memory=20g" \
  --files ./triplets.soda2103 ./target/wQFMSpark-1.0-SNAPSHOT-jar-with-dependencies.jar
#  --conf "spark.task.cpus=4" \
