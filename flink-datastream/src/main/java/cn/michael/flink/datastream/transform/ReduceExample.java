package cn.michael.flink.datastream.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hufenggang on 2020/9/6.
 */
public class ReduceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(
            new Tuple2<>("a", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("b", 3),
            new Tuple2<>("c", 2));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = dataStream.keyBy(value -> value.f0);

        keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
            }
        }).print();

        env.execute("ReduceExample");
    }
}
