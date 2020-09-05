package cn.michael.flink.datastream.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hufenggang on 2020/9/4.
 */
public class FilterExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 2, 3, 4, 5).filter(x -> x % 2 == 0).print();

        env.execute();
    }
}
