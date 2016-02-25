package sql

import java.io.StringWriter

import au.com.bytecode.opencsv.CSVWriter
import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object BytesCountJob {

  def main(args: Array[String]) {

    val input = "/home/normal/Documents/hadoop epam/spark/access_logs/input/000000"
    val output = "/home/normal/Documents/hadoop epam/spark/access_logs/input/111111"

    val conf = new SparkConf().setMaster("local").setAppName("Bytes count")
    val sc = new SparkContext(conf)

    val operaAcc = sc.accumulator(0, "Opera")
    val msieAcc = sc.accumulator(0, "IE")
    val chromeAcc = sc.accumulator(0, "Chrome")
    val firefoxAcc = sc.accumulator(0, "Firefox")

    val rdd = sc.textFile(input)
    transformInput(rdd)
      .mapPartitions {
                       x =>
                         val stringWriter = new StringWriter()
                         val csvWriter = new CSVWriter(stringWriter)
                         csvWriter.writeAll(x.toList.asJava)
                         Iterator(stringWriter.toString)
                     }.saveAsTextFile(output)

    rdd.foreach {
                  x =>
                    val tokens = x.split("\\s")
                    if (tokens.size > 11) {
                      val userAgent = new UserAgent(tokens(11))
                      val browser = userAgent.getBrowser.getName

                      if(browser.contains("Opera")) operaAcc += 1
                      if(browser.contains("Explorer")) msieAcc += 1
                      if(browser.contains("Chrome")) chromeAcc += 1
                      if(browser.contains("Mozilla")) firefoxAcc += 1
                    }

                }

    println(s"Opera: $operaAcc")
    println(s"IE: $msieAcc")
    println(s"Chrome: $chromeAcc")
    println(s"Firefox: $firefoxAcc")

    println("Done!")
  }

  def transformInput(input: RDD[String]): RDD[Array[String]] = {
    input
      .map {
             x =>
               val tokens = x.split("\\s")
               (tokens(0), tokens(9))
           }
      // bytes should be numeric
      .filter(x => isNum(x._2))
      // group by ip -> (ip, List(byte1, byte2, ...))
      .groupBy(_._1)
      // (ip, List(byte1, byte2, ...)) -> (ip, bytes total, requests size)
      .map {
             x =>
               val sum = x._2.map(_._2.toInt).sum
               val count = x._2.size
               val average = sum / count
               Array[String](x._1, average.toString, sum.toString)
           }
  }

  def isNum(str: String): Boolean = {
    try {
      str.toInt
      true
    } catch {
      case e: NumberFormatException => false
    }
  }
}
