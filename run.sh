spark-submit \
  --class NycTaxiHourlyJob \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.yarn.maxAppAttempts=1 \
  target/scala-2.11.12/nyc-taxi-hourly-weather.jar