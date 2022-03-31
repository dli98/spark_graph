package utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSpark = {
    SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
  }
}
