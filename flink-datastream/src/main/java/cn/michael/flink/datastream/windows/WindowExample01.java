package cn.michael.flink.datastream.windows;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hufenggang on 2021/5/26.
 */
public class WindowExample01 {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 如果不指定时间的话，默认为 ProcessingTime
        // 如果指定为事件时间，需要事件中带有时间或者添加时间水印
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        
    }
}
