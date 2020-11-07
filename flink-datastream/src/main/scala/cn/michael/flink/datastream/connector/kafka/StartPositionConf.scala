package cn.michael.flink.datastream.connector.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Created by hufenggang on 2020/4/22.
 */
object StartPositionConf {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val myConsumer = new FlinkKafkaConsumer[String]((
      java.util.regex.Pattern.compile("test-topic-[0-9]")),
      new SimpleStringSchema,
      properties)

    myConsumer.setStartFromEarliest()      // start from the earliest record possible
    myConsumer.setStartFromLatest()        // start from the latest record
    myConsumer.setStartFromTimestamp(5000)  // start from specified epoch timestamp (milliseconds)
    myConsumer.setStartFromGroupOffsets()  // the default behaviour

    val stream = env.addSource(myConsumer)
  }

}
