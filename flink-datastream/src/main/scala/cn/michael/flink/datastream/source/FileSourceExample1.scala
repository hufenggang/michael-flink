package cn.michael.flink.datastream.source

import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Created by hufenggang on 2020/5/24.
 */
class FileSourceExample1 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 直接读取文本文件
    val textStream: DataStreamSource[String] = env.readTextFile("/user/local/flink/data_example.log")
    // 通过指定CSVInputFormat读取CSV文件
    val csvStream: DataStreamSource[String] = env.readFile(new CsvInputFormat[String](new Path("/user/local/flink/data_example.csv")) {
      override def fillRecord(out: String, objects: Array[AnyRef]) = {
        null
      }
    }, "/user/local/flink/data_example.csv")
  }

}
