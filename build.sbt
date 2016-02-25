import _root_.sbtassembly.Plugin.AssemblyKeys._
import _root_.sbtassembly.Plugin._

name := "spark-examples"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  // Spark dependency
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.2" % "provided",

  // CSV utils
  "net.sf.opencsv" % "opencsv" % "2.3",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",

  // Tcp package analyzer
  "org.pcap4j" % "pcap4j-core" % "1.6.1",
  "org.pcap4j" % "pcap4j-packetfactory-static" % "1.6.1",

  // User-agent parser
  "eu.bitwalker" % "UserAgentUtils" % "1.18",

  // Unit testing
  "com.holdenkarau" % "spark-testing-base_2.10" % "1.5.1_0.2.1" % "test",

  // KAFKA
  "org.apache.kafka" % "kafka_2.11" % "0.8.2.0"
)

// This statement includes the assembly plug-in capabilities
assemblySettings

// Configure JAR used with the assembly plug-in
jarName in assembly := "spark-examples.jar"

// A special option to exclude Scala itself form our assembly JAR, since Spark
// already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
    