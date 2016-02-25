package streaming

import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, KafkaProducer}

object KafkaUtils {

  val kafkaProps = new java.util.HashMap[String, Object]() {
    {
      put("zk.connect", "localhost:2181")
      put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    }
  }
  val producer = new KafkaProducer[String, String](kafkaProps)
  val topic: String = "alerts"

  def send(message: String) {
    producer.send(new ProducerRecord[String, String](topic, null, message))
  }
}

case class IncidentMessage(ip: String, actualValue: Int, limit: Int, period: Int) {

  def exceeded: String = {
    s"(EXCEEDED ${System.currentTimeMillis() / 1000}) IP: $ip, BYTES: $actualValue, LIMIT (bytes): $limit, PERIOD (sec): $period"
  }

  def normalized: String = {
    s"(NORMALIZED ${System.currentTimeMillis() / 1000}) IP: $ip, BYTES: $actualValue, LIMIT (bytes): $limit, PERIOD (sec): $period"
  }
}
