package cn.michael.flink.datastream.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by hufenggang on 2020/11/14.
 *
 * 全局窗口
 */
public class WindowAllExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(
            new Tuple2<>("a", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("b", 3),
            new Tuple2<>("c", 2));

        AllWindowedStream<Tuple2<String, Integer>, TimeWindow> windowStream = dataStream
            .keyBy(value -> value.f0)
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        windowStream.max(0).print();

        env.execute("WindowAllExample");
    }
}
