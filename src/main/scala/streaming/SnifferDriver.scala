package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.sql.SQLContext

object SnifferDriver extends Logging {

  val DEFAULT_SETTING_KEY = "NULL"

  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming example")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint(".")
    implicit val sqlContext = new SQLContext(ssc.sparkContext)

    val limitSettings = ConfigLoader.load(2).get(DEFAULT_SETTING_KEY).get
    val period: Int = limitSettings._2.toInt

    val stream = ssc.receiverStream(new SnifferReceiver())
      .map(x => (x.ip, x.bytes))
      .reduceByKey(_ + _)

    // Analyzing
    stream.window(Seconds(period), Seconds(period))
      // Shout-out to A.I. for that solution :)
      .updateStateByKey(
        (bytes: Seq[Int], state: Option[(Int, Int)]) => {
          val current = bytes.sum
          val previous = state.getOrElse((0, 0))._2

          Some((previous, current))
        })
      .foreachRDD(rdd => {
      rdd.collect().foreach(
        x => {
          val ip = x._1
          val values = x._2
          val bytes = values._2
          val previous = values._1

          // TODO: threshold can be analyzed here in the same way ...
          val limit: Int = limitSettings._1.toInt
          val limitExceeded = bytes >= limit
          val previousState = previous >= limit

          if (limitExceeded && !previousState) {
            // exceeded
            KafkaUtils.send(IncidentMessage(ip, bytes, limit, period).exceeded)
          } else if (!limitExceeded && previousState) {
            // normalized
            KafkaUtils.send(IncidentMessage(ip, bytes, limit, period).normalized)
          }
          println(s"$ip, $bytes, $limitExceeded")

        })
    })

    // Statistics
    // TODO: change Seconds to Minutes
    stream.window(Seconds(60), Seconds(60))
      .map(
        x => {
          val ip = x._1
          val bytes = x._2
          val speed = bytes / 60d

          (System.currentTimeMillis() / 1000, ip, bytes, speed)
        })
    .saveAsTextFiles("/home/normal/dev/projects/bigdata/spark-hw2/src/main/resources/output.csv")

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
