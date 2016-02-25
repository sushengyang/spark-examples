package streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode
import org.pcap4j.core.Pcaps
import org.pcap4j.packet.{IpV4Packet, EthernetPacket}

class SnifferReceiver extends Receiver[Packet](StorageLevel.MEMORY_ONLY) {

  override def onStart(): Unit = {
    new Thread() {
      setDaemon(true)
      receive()
    }.start()
  }

  def receive() {
    val nif = Pcaps.getDevByName("eth0")

    val handle = nif.openLive(1024, PromiscuousMode.PROMISCUOUS, 0)

    while (!isStopped()) {
      val packet = handle.getNextPacket.asInstanceOf[EthernetPacket]
      if (packet != null) {
        val payload = packet.getPayload

        payload match {
          case packet: IpV4Packet =>
            val header = packet.getHeader

            store(Packet(header.getDstAddr.getHostAddress, header.getTotalLengthAsInt))
          case _ =>
        }
      }
    }
  }

  override def onStop(): Unit = {}
}
case class Packet(ip: String, bytes: Int) extends Serializable
