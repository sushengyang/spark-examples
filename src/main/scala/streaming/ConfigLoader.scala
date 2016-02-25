package streaming

import org.apache.spark.sql.SQLContext

object ConfigLoader {

  def load(eventType: Int)(implicit sqlContext: SQLContext): scala.collection.Map[String, (String, String)] = {
    val settings = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("/src/main/resources/settings.csv")
      .cache()

    settings.filter(settings("Type") === eventType)
      .map(df => (df(0).toString, (df(2).toString, df(3).toString)))
      .collectAsMap()
  }
}
