package cn.michael.flink.sql.base;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/**
 * Created by hufenggang on 2020/11/8.
 *
 * Flink老版本的批式查询
 */
public class Example01FlinkBatchQuery {

    public static void main(String[] args) {
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);
    }
}
