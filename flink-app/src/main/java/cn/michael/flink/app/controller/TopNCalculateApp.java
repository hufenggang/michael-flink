package cn.michael.flink.app.controller;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Created by hufenggang on 2021/1/23.
 */
public class TopNCalculateApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties pros = new Properties();
        pros.setProperty("bootstrap.servers", "127.0.0.1:9092");
        pros.setProperty("group.id", "test");
    }
}
