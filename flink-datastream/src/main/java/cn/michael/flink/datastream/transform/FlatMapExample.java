package cn.michael.flink.datastream.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by hufenggang on 2020/9/3.
 */
public class FlatMapExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String string01 = "java java java scala";
        String string02 = "spark spark flink flink flink";

        DataStreamSource<String> stringDataStreamSource = env.fromElements(string01, string02);

        stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }
        }).print();

        env.execute();
    }
}
