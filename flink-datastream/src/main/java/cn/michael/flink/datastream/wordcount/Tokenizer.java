package cn.michael.flink.datastream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * Created by hufenggang on 2021/1/3.
 */
public class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.toLowerCase().split("\\W+");

        for (String word : words) {
            if (words.length > 0) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
