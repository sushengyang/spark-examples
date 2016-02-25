package mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.{Algo, Strategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf, Logging}

object BankOfferPrediction extends Logging {

  def main(args: Array[String]) {
    Logger.getRootLogger setLevel Level.WARN

    val splits: Array[RDD[LabeledPoint]] = BankOfferPrediction.setUp(args)
    val training = splits(0) cache()
    val test = splits(1) cache()

    val prediction = decisionTree(training, test)

    val auROC = results(test, prediction)
    println("Area under ROC = " + auROC)
  }

  def results(test: RDD[LabeledPoint], prediction: RDD[Double]): Double = {
    // results
    val predictionAndLabel = prediction zip (test map (_ label))
    predictionAndLabel.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))

    // analysis
    val metrics = new BinaryClassificationMetrics(predictionAndLabel)
    val auROC = metrics.areaUnderROC()
    auROC
  }

  def decisionTree(training: RDD[LabeledPoint], test: RDD[LabeledPoint]): RDD[Double] = {
    val algorithm = new DecisionTree(Strategy.defaultStrategy(Algo.Regression))
    val model = algorithm run training
    val prediction = model predict (test map (_ features))
    prediction
  }

  def getFeatures(row: Row): org.apache.spark.mllib.linalg.Vector = {
    def asInt(index: Int) = row.getInt(index).toDouble
    def asDouble(index: Int) = row.getString(index).replace(",", ".").toDouble
    def asDoubleNan(index: Int) = if (row.getDouble(index) == Double.NaN) 0d
    else row.getDouble(index).toInt.toDouble

    val features = Map[Int, Int => Double](
      0 -> asInt, // age
      1 -> asInt, // is employed
      2 -> asInt, // is retired
      3 -> asInt, // sex
      4 -> asInt, // children
      5 -> asInt, // dependent people
//      6 -> asInt, // education
//      7 -> asInt, // marital status
//      12 -> asDoubleNan, // direction of activity inside the company
      14 -> asDouble, // personal income
      19 -> asDoubleNan, // state
      32 -> asDouble, // last loan
      38 -> asDoubleNan // living months
    )

    Vectors.dense(features.map {case (k, v) => v(k)}.toArray)
  }

  def load(file: String)(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load(file)
      .cache()
  }



  def setUp(args: Array[String]): Array[RDD[LabeledPoint]] = {
    // configuration
    val conf = new SparkConf() setMaster "local[2]" setAppName "Bank offer prediction driver"
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)
    val N = if (args.length > 0) {
      args(0).toInt
    } else {
      100
    }

    // prepare input
    val objects = BankOfferPrediction.load("Objects.csv").limit(N)
    val targets = BankOfferPrediction.load("Target.csv").limit(N)

    val input = targets.rdd.zip(objects.rdd)
      .map(x => LabeledPoint(x._1.getInt(0), BankOfferPrediction.getFeatures(x._2)))
    val splits = input randomSplit Array(0.6, 0.4)
    splits
  }

}
