spark-submit \
  --class NycTaxiHourlyJob \
  --master yarn --deploy-mode cluster \
  --driver-memory 4g --executor-memory 4g --executor-cores 2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.yarn.maxAppAttempts=1 \
  /usr/local/spark/jobs/nyc-taxi-hourly-assembly.jar