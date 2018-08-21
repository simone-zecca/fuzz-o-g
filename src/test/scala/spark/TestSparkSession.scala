package spark

import org.apache.spark.sql.SparkSession

trait TestSparkSession {

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "local[*]")
    .config("spark.executor.memory", "3g")
    .getOrCreate()

}
