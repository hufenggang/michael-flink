package cn.michael.flink.datastream.connector.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Created by hufenggang on 2020/4/22.
 */
object Example01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
      .print()

    env.execute("test")
  }

}
