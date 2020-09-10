package cn.michael.flink.datastream.transform;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hufenggang on 2020/9/6.
 */
public class FoldExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(
            new Tuple2<>("a", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("b", 3),
            new Tuple2<>("c", 2));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = dataStream.keyBy(value -> value.f0);
        keyedStream.fold("start", new FoldFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String fold(String s, Tuple2<String, Integer> o) throws Exception {
                return s + "-" + o;
            }
        }).print();

        env.execute("FoldExample");

    }
}
