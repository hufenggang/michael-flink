package cn.michael.flink.datastream.windows;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Created by hufenggang on 2020/12/14.
 *
 * 滚动窗口示例
 */
public class TumblingWindowsExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();;

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("topic-test", new SimpleStringSchema(), pros));

        stream.keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum(1);

        stream.keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        stream.keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-1)))
                .sum(1);

        env.execute("TumblingWindowsExample");

    }
}
