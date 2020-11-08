package cn.michael.flink.sql.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by hufenggang on 2020/11/5.
 *
 * Flink老版本的流式查询
 */
public class Example02FlinkStreamingQuery {

    public static void main(String[] args) {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
            .useOldPlanner()
            .inBatchMode()
            .build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

    }
}
