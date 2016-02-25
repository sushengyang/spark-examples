package sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsJob {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Bytes count")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    val year = load("20072.csv")
    val carriers = load("carriers.csv")
    val airports = load("airports.csv")

    year.registerTempTable("year")
    carriers.registerTempTable("carriers")
    airports.registerTempTable("airports")

    year.join(carriers, year("UniqueCarrier") === carriers("Code"))
      .groupBy(carriers("Description"))
      .count().show()
    println("Count total number of flights per carrier in 2007")

    val servedByNyc =
      """select count(*)
         from year y
         join airports a1 ON y.Origin = a1.iata
         join airports a2 ON y.Dest = a2.iata
         where y.Month = 7 AND (a1.city = 'New York' or a2.city = 'New York')
      """
    sqlContext.sql(servedByNyc).show()
    println("The total number of flights served in Jun 2007 by NYC")

    val busyAirports =
      """select sum(count) as result, airport
         from (
           select count(*) as count, a.airport
           from year as y join airports as a on y.Dest = a.iata
           where y.Month in (6,7,8) and a.country = 'USA'
           group by a.airport

           UNION ALL

           select count(*) as count, a.airport
           from year as y join airports as a on y.Origin = a.iata
           where y.Month in (6,7,8) and a.country = 'USA'
           group by a.airport) as un
         group by airport
         order by result DESC
         limit 5
      """
    sqlContext.sql(busyAirports).show()
    println("Find five most busy airports in US during Jun 01 - Aug 31")

    year.join(carriers, carriers("Code") === year("UniqueCarrier"))
      .groupBy(carriers("Description"))
      .count().sort(desc("count"))
      .limit(1).show()

    println("Find the carrier who served the biggest number of flights")
  }

  def load(file: String)(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(s"airports.carrier.dataset/$file")
      .cache()
  }

}
