package com.jaywong.hive;

import com.jaywong.util.PropertiesUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.tools.ant.taskdefs.Length;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.descriptors.TableDescriptor;
import java.lang.reflect.Array;
import java.time.Duration;

/**
 * @author wangjie
 * @create 2023-03-31 17:08
 */
public class HiveImpl {
    private final static Logger logger = LoggerFactory.getLogger(HiveImpl.class);
    public static void main(String[] args) throws Exception {
//        读取配置
        PropertiesUtils instance = PropertiesUtils.getInstance();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,
                CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,
                Duration.ofSeconds(20));

        HiveCatalog catalog = new HiveCatalog(
                instance.getCatalogName(),
                instance.getHiveDatabase(),
                instance.getHiveConfDir()
        );
//        tableEnv.registerCatalog("default_catalog", catalog);
        tableEnv.useCatalog(instance.getCatalogName());
        String[] strings = tableEnv.listCatalogs();
        tableEnv.executeSql("CREATE CATALOG myhive WITH ('type' = 'hive', 'default-database' = 'myhive', 'hive-conf-dir' = 'D:\\CodeEnv\\BigData\\apache-hive-2.1.1-bin\\conf') ");
        for (int i=0;i <= 10; i++) {
            System.out.println(strings[i]);
        }

        tableEnv.executeSql("DROP TABLE IF EXISTS db_test_tmp.test_kafka_source");
        tableEnv.executeSql("CREATE TABLE db_test_tmp.test_kafka_source (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  statdate STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka'," +
                " 'topic' = 'kafka_test',\n" +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'properties.group.id' = 'hive_test_09',\n" +
                " 'format' = 'json',\n"+
                " 'scan.startup.mode' = 'latest-offset')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("DROP TABLE  db_test_tmp.test_hive_sink");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  db_test_tmp.test_hive_sink (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT\n" +
                ") PARTITIONED BY (statdate String) STORED AS PARQUET\n" +
                "TBLPROPERTIES (\n" +
                "  'partition.time-extractor.timestamp-pattern'='$statdate 00:00:00',\n" +
                "  'stream-source.consume-order' = 'partition-time',\n" +
                "  'stream-source.enable' = 'true',\n" +
                "  'sink.partition-commit.policy.kind' = 'metastore,success-file'\n" +
                ")");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("INSERT INTO db_test_tmp.test_hive_sink SELECT  id,name,age,DATE_FORMAT(statdate,'yyyy-MM-dd') FROM test_kafka_source");
    }
}
