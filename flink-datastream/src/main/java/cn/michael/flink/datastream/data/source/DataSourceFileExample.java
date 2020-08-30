package cn.michael.flink.datastream.data.source;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * Created by hufenggang on 2020/8/30.
 *
 * 基于文件构建数据源
 */
public class DataSourceFileExample {

    public static void main(String[] args) throws Exception {
        final String filePath = "/Users/hufenggang/develop/codes/other/log/log4j.properties";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readFile(new TextInputFormat(new Path(filePath)),
            filePath,
            FileProcessingMode.PROCESS_ONCE,
            1,
            BasicTypeInfo.STRING_TYPE_INFO).print();

        env.execute();
    }
}
