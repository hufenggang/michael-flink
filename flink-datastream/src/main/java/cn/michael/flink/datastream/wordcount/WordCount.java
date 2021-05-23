package cn.michael.flink.datastream.wordcount;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * Created by hufenggang on 2021/1/3.
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 设置运行时环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
            .keyBy(value -> value._2())
            .sum(1);

        counts.print();

        env.execute("Streaming WordCount");

    }
}
