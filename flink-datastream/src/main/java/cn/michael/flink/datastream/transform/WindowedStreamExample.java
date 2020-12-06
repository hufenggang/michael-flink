package cn.michael.flink.datastream.transform;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by hufenggang on 2020/11/14.
 */
public class WindowedStreamExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(
            new Tuple2<>("a", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("b", 3),
            new Tuple2<>("c", 2));

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = dataStream
            .keyBy(value -> value.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)));


        env.execute("WindowAllExample");
    }
}
