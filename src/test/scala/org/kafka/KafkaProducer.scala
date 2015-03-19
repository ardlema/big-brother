package org.kafka

import java.util.Properties

import kafka.message.Message
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.test.TestingServer

trait KafkaProducer {
  val zookeeperCluster = new TestingServer(2181)
  zookeeperCluster.start

  val props = new Properties()
  val brokerId = "1"
  props.setProperty("zookeeper.connect", "localhost:2181")
  props.setProperty("broker.id", brokerId)
  props.setProperty("host.name", "localhost")
  props.setProperty("port", Integer.toString(9092))
  props.setProperty("log.dir", "/tmp/kafka")
  props.setProperty("log.flush.interval.messages", String.valueOf(1))
  props.put("metadata.broker.list", "localhost:9092")
  props.put("auto.leader.rebalance.enable", "true")
  props.put("controlled.shutdown.enable", "true")
  val server = new KafkaServer(new KafkaConfig(props))
  server.startup()

  val propsProd = new Properties()
  propsProd.put("metadata.broker.list", "localhost:9092")
  propsProd.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, String](config)

  def withKakfaProducer(p: Producer[String, Message] => Any) =  {
    val prodConfig = new ProducerConfig(propsProd)
    val producer = (new Producer[String, Message](prodConfig))
    p(producer)
    producer.close
    server.shutdown()
    server.awaitShutdown()
    zookeeperCluster.stop()
  }
}
