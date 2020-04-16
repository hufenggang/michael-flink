package cn.michael.flink.examples.streaming.time

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Created by hufenggang on 2020/4/13.
 */
object ProcessingTimeExample01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  }

}
