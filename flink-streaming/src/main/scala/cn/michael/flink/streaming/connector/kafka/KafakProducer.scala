package cn.michael.flink.streaming.connector.kafka

import java.util.Properties

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * Created by hufenggang on 2020/4/24.
 */
object KafakProducer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val myProducer = new FlinkKafkaProducer011[String]()
    myProducer.setWriteTimestampToKafka(true)

    stream.addSink(myProducer)
  }

}
