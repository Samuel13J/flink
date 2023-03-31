package com.jaywong.flink;

import akka.japi.tuple.Tuple4;
import com.jaywong.mysql.MysqlImpl;
import com.jaywong.util.PropertiesUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wangjie
 * @create 2023-03-29 18:11
 */
public class KafkaToMysql implements Serializable {
    public static void main(String[] args) throws Exception {

//        读取配置
        PropertiesUtils instance = PropertiesUtils.getInstance();
//        kafka配置参数
        String topic = instance.getKafkaTopic();
        Properties kafkaConf = new Properties();
        kafkaConf.put(ConsumerConfig.GROUP_ID_CONFIG, instance.getgroupID());
        kafkaConf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, instance.getkafkaBootStrapServers());
        kafkaConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, instance.getenableAutoCommit());
        kafkaConf.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, instance.getmaxPollRecords());
        kafkaConf.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, instance.getautoCommitIntervalMS());
        kafkaConf.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, instance.getsessionTimeoutMS());
        kafkaConf.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, instance.getrequestTimeoutMS());
        kafkaConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, instance.getKafkaConsumerKeyDeserializer());
        kafkaConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, instance.getKafkaConsumerValueDeserializer());

//        Flink
//        获取流执行环境
        StreamExecutionEnvironment envs = StreamExecutionEnvironment.getExecutionEnvironment();
        envs.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        重启策略之固定间隔（fixed delay）
        envs.setRestartStrategy(RestartStrategies.fixedDelayRestart(instance.getFlinkFixedDelayRestartTimes(),
                Time.of(instance.getFlinkFixedDelayRestartInterval(), TimeUnit.MINUTES)));
//        设置间隔多长产生checkpoint
        envs.enableCheckpointing(instance.getFlinkCheckpointInterval());
//        设置模式为excetly-once（默认值）
        envs.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        确保检查点之间至少有 500ms的间隔【checkpoint最小间隔】
//        envs.getCheckpointConfig().setMinPauseBetweenCheckpoints(instance.getFlinkCheckpointMinPauseBetweenCheckpoints());
//        检查点必须在一分钟之内完成，或者被丢弃【checkpoint的超时时间】
        envs.getCheckpointConfig().setCheckpointTimeout(instance.getFlinkCheckpointCheckpointTimeout());
//        同一时间只允许进行一个检查点
        envs.getCheckpointConfig().setMaxConcurrentCheckpoints(instance.getFlinkCheckpointMaxConcurrentCheckpoint());
//        表示一旦flink处理程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        envs.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        envs.setParallelism(1);

//        flink table
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(envs, settings);

//        添加kafka source
        DataStreamSource<String> mykafka = envs.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema()
                , kafkaConf));
        mykafka.print();

        DataStream<akka.japi.tuple.Tuple4<String, Integer, String, String>> stream = mykafka.map(new MapFunction<String, akka.japi.tuple.Tuple4<String,
                Integer, String, String>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public akka.japi.tuple.Tuple4<String, Integer, String, String> map(String value) throws Exception {
                String[] strings = value.split(",");
                return new Tuple4<String, Integer, String, String>(strings[0],Integer.parseInt(strings[1]),strings[2],
                        strings[3]);
            }
        });
        stream.addSink(new MysqlImpl());
        envs.execute();
    }
}