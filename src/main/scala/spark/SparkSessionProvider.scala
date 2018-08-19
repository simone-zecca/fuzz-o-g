package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()

}
