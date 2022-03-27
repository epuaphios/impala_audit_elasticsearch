import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




val elementSchema = StructType(Array(
  StructField("name",StringType,true),
  StructField("object_type",StringType,true),
  StructField("privilege",StringType,true)
))

val simpleSchema = new StructType()
  .add("authorization_failure",StringType,true)
  .add("catalog_objects",elementSchema,true)
  .add("impersonator",StringType,true)
  .add("network_address",StringType,true)
  .add("query_id",StringType,true)
  .add("session_id",StringType,true)
  .add("sql_statement",StringType,true)
  .add("start_time",StringType,true)
  .add("statement_type",StringType,true)
  .add("status",StringType,true)
  .add("user",StringType,true)

val anaSchema = new StructType()
  .add("saasd",StringType,true)

val config = new SparkConf()
config.set("spark.sql.shuffle.partitions","300")

val spark=SparkSession.builder().config(config).master("local[1]")
  .appName("SparkByExamples")
  .getOrCreate()




val ds: Dataset[String] = spark.read.textFile("/home/ogn/denemeler/big_data/impala_audit_spark/file/testa.txt")

ds.show()

import spark.implicits._
val ds2: Dataset[String] = ds.withColumn("value", regexp_extract('value, "\\{.*:(\\{.*\\})\\}", 1)).as[String]

val df3: DataFrame = spark.read.json(ds2)


val df4: DataFrame = df3.select(df3.col("authorization_failure").as("authorization_failure"),
  col("impersonator").as("personName"),
  col("network_address").as("network_address"),
  col("query_id").as("query_id"),
  col("session_id").as("session_id"),
  col("sql_statement").as("personName"),
  col("start_time").as("start_time"),
  col("user").as("user"),
  col("statement_type").as("statement_type"),
org.apache.spark.sql.functions.explode(df3.col("catalog_objects")).as("catalog_objects"))

df4.show()

val df5 = df4.select(col("authorization_failure").as("authorization_failure"),
  col("impersonator").as("personName"),
  col("network_address").as("network_address"),
  col("query_id").as("query_id"),
  col("session_id").as("session_id"),
  col("sql_statement").as("personName"),
  col("start_time").as("start_time"),
  col("user").as("user"),
  col("statement_type").as("statement_type"),
  df4.col("catalog_objects").getField("name").as("dbname"),
  df4.col("catalog_objects").getField("object_type").as("object_type"));


df5.show()
