package cn.michael.flink.datastream.connector.kafka

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Created by hufenggang on 2020/4/22.
 */
object TopicDiscoveryExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer[String]((
      java.util.regex.Pattern.compile("test-topic-[0-9]")),
      new SimpleStringSchema,
      properties)

    val stream = env.addSource(consumer)
  }

}
