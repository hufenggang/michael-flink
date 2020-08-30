package cn.michael.flink.examples.streaming.operator.windows

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Created by hufenggang on 2020/5/17.
 */
object TumblingWindowsExample01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStreamSource[String] = env.socketTextStream("localhost", 9999)
    new KeySelector[] {}

    input.keyBy("")
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))

  }

}
