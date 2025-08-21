name := "NycTaxiHourlyJob"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "3.5.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)