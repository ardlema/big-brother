package org.kafka

import java.util.Properties

import kafka.message.Message
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.test.TestingServer

trait KafkaProducer {
 
  def withKafkaProducer(executionBlock: Producer[String, String] => Any) =  {
    val zookeeperCluster = createZookeeperCluster
    val kafkaBroker = createKafkaBroker
    val kafkaProducer = createKafkaProducer
    initializeServices(zookeeperCluster, kafkaBroker)
    executionBlock(kafkaProducer)
    shutdownServices(zookeeperCluster, kafkaBroker, kafkaProducer)
  }
  
  def createZookeeperCluster =  new TestingServer(2181)

  def createKafkaBroker = {
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
    new KafkaServer(new KafkaConfig(props))
  }
  
  def createKafkaProducer = {
    val propsProd = new Properties()
    propsProd.put("metadata.broker.list", "localhost:9092")
    propsProd.put("request.required.acks", "1")
    propsProd.put("serializer.class","kafka.serializer.StringEncoder")
    val prodConfig = new ProducerConfig(propsProd)
    new Producer[String, String](prodConfig)
  }
  
  def initializeServices(zkCluster: TestingServer, kafkaBroker: KafkaServer) = {
    zkCluster.start
    kafkaBroker.startup
  }

  def shutdownServices(zkCluster: TestingServer,
                       kafkaBroker: KafkaServer,
                       kafkaProducer: Producer[String, String]) = {
    kafkaProducer.close
    kafkaBroker.shutdown
    kafkaBroker.awaitShutdown
    zkCluster.stop
  }
}
