package cn.michael.flink.datastream.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hufenggang on 2020/9/4.
 */
public class KeyByExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromElements(
            new Tuple2<>("a", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("b", 3),
            new Tuple2<>("c", 2));
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = tuple2DataStreamSource.keyBy(0);
        keyedStream.reduce((ReduceFunction<Tuple2<String, Integer>>)(value1, value2) ->
            new Tuple2<>(value1.f0, value1.f1 + value2.f1)).print();
        env.execute();
    }
}
