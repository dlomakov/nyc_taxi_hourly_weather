import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object NycTaxiHourlyJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("nyc-taxi-hourly")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Параметры запуска
    val tripsPath   = sys.env.getOrElse("TRIPS_PATH", "hdfs:///data/tlc/yellow/yellow_tripdata_2025-01.parquet")
    val zonesPath   = sys.env.getOrElse("ZONES_PATH", "hdfs:///data/tlc/taxi_zone_lookup.csv")
    val weatherPath = sys.env.getOrElse("WEATHER_PATH","hdfs:///data/weather/nyc_hourly.parquet")
    val fromDate    = sys.env.getOrElse("FROM_DT","2025-01-01")
    val toDate      = sys.env.getOrElse("TO_DT","2025-01-31")

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("SET spark.sql.parquet.compression.codec=snappy")

    // Чтение зон
    val zones = spark.read
      .option("header","true").option("inferSchema","true")
      .csv(zonesPath)
      .select(
        col("LocationID").cast("int").as("zone_id"),
        col("Borough").as("borough"),
        col("Zone").as("zone_name")
      )

    // Унифицированный ридер поездок (CSV или Parquet)
    val tripsRaw =
      spark.read.format("parquet").load(tripsPath)
        .option("basePath", tripsPath)
        .selectExpr(
          "cast(VendorID as int) as vendor_id",
          "tpep_pickup_datetime as pickup_ts",
          "tpep_dropoff_datetime as dropoff_ts",
          "cast(passenger_count as int) as passenger_count",
          "cast(trip_distance as double) as trip_miles",
          "cast(PULocationID as int) as pu_location_id",
          "cast(DOLocationID as int) as do_location_id",
          "cast(total_amount as double) as total_amount",
          "cast(payment_type as int) as payment_type"
        )
        .where(col("pickup_ts").between(lit(fromDate), lit(toDate)))
	
    // Очищаем данные, высчитываем время поездки
    val tripsClean = tripsRaw
      .withColumn("duration_min",
        (unix_timestamp($"dropoff_ts") - unix_timestamp($"pickup_ts")) / 60.0
      )
      .withColumn("speed_kmh",
        when($"duration_min" > 0,
          ($"trip_miles" * lit(1.60934)) / ($"duration_min" / 60.0)
        ).otherwise(lit(null).cast("double"))
      )
      .filter($"duration_min".between(1, 180) && $"trip_miles" > 0 && $"speed_kmh" < 120)
      .dropDuplicates("pickup_ts","dropoff_ts","pu_location_id","do_location_id","total_amount")

    // Обогащаем данные, парсим информацию о времени и добавляем зоны из другого датасета
    val tripsEnriched = tripsClean
      .withColumn("pickup_hour", date_trunc("hour", $"pickup_ts"))
      .withColumn("dt", to_date($"pickup_ts"))
      .join(zones, $"pu_location_id" === zones("zone_id"), "left")

    // Погода должна иметь столбец hour_ts (TIMESTAMP, начало часа)
    val weather = spark.read.format("parquet").load(weatherPath)
      .select(
        col("hour_ts").as("pickup_hour"),
        col("temp_c").cast("double"),
        col("precip_mm").cast("double")
      )

    // Добавляем к нашим данным о поездках и зонах информацию о погоде
    val withWeather = tripsEnriched.join(weather, Seq("pickup_hour"), "left")

    // Проводим почасовую агрегацию
    val hourlyAgg = withWeather
      .groupBy($"zone_id", $"borough", $"pickup_hour", $"dt")
      .agg(
        count(lit(1)).as("trips_cnt"),
        avg($"duration_min").as("avg_duration_min"),
        expr("percentile_approx(duration_min, 0.5, 100)").as("p50_duration_min"),
        expr("percentile_approx(duration_min, 0.95, 100)").as("p95_duration_min"),
        avg($"speed_kmh").as("avg_speed_kmh"),
        sum($"total_amount").as("revenue_usd"),
        (sum(when($"payment_type" === 2, 1).otherwise(0)) / count(lit(1))).as("cash_share"),
        avg($"temp_c").as("temp_c"),
        sum($"precip_mm").as("precip_mm")
      )

    // Скользящим окном в 7d высчитываем среднее по часам
    val wRolling = Window.partitionBy($"zone_id").orderBy($"pickup_hour").rowsBetween(168, 0)
    val hourlyWithRolling = hourlyAgg
      .withColumn("trips_cnt_roll7d", avg($"trips_cnt").over(wRolling))

    // Запись в Hive (партиционирование по dt)
    hourlyWithRolling
      .select(
        	$"zone_id",$"borough",$"pickup_hour",
        	$"trips_cnt",$"avg_duration_min",$"p50_duration_min",$"p95_duration_min",
        	$"avg_speed_kmh",$"revenue_usd",$"cash_share",
        	$"temp_c",$"precip_mm",
        	$"dt".cast("string").as("dt")
      	)
      	.write
      	.mode("append")
      	.format("hive")
     	.insertInto("dwh.fact_taxi_hourly")

    // Высчитываем Топ‑10 зон по дням
    val daily = hourlyAgg.groupBy($"zone_id", $"borough", $"dt")
      .agg(sum($"trips_cnt").as("trips_day"))
    val topDaily = daily
      .withColumn("rank",
        rank().over(Window.partitionBy($"dt").orderBy(desc("trips_day")))
      ).filter($"rank" <= 10)

    topDaily.createOrReplaceTempView("top_zones_daily")
    spark.sql("CREATE OR REPLACE VIEW dwh.v_top_zones_daily AS SELECT * FROM top_zones_daily")

    spark.stop()
  }
}