package cn.michael.flink.sql.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created by hufenggang on 2020/11/8.
 * <p>
 * 创建视图
 */
public class Example05CreateTableView {

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        Table table = tableEnv.from("").select($("id"), $("name"));
        tableEnv.createTemporaryView("v_table", table);

    }
}
