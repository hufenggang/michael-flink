package cn.michael.flink.datastream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created by hufenggang on 2021/5/23.
 */
public class SocketTextStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 参数检查
        if (args.length !=2 ) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        // 设置环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据
        DataStream<String> stream = env.socketTextStream(hostname, port);
        // 计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream
            .flatMap(new LineSplitter())
            .keyBy(value -> value.f0)
            .sum(1);

        result.print();

        env.execute("SocketTextStreamWordCount");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }
    }
}
