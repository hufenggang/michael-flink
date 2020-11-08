package cn.michael.flink.sql.base;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Created by hufenggang on 2020/11/8.
 *
 * Blink版本的批式查询
 */
public class BlinkBatchQuery {

    public static void main(String[] args) {
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
    }
}
