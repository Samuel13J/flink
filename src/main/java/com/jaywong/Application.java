package com.jaywong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jaywong.sink.HbaseSink;
import com.jaywong.trigger.CountTrigger;
import com.jaywong.util.HttpDataModel;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author wangjie
 * @create 2023-04-23 16:04
 */
public class Application {

    @SuppressWarnings(value={"unchecked"})
    public static void main(String[] args) throws Exception{
//        kafka需要的参数
        String brokers = "";
        String username = "";
        String password = "";
        String topic = "";
//        hbase需要的参数
        String hbase_zookeeper_host = "";
        String hbase_zookeeper_port = "";

//        接受命令行参数，覆盖默认值
        Options options = new Options();
        options.addOption("kafka_brokers", true, "kafka cluster hosts, such 127.0.0.1:9092");
        options.addOption("kafka_username", true, "kafka cluster username, default: admin");
        options.addOption("kafka_user_password", true, "kafka cluster user password, default: 123456");
        options.addOption("kafka_topic", true, "kafka cluster topic, default: test");

        options.addOption("hbase_zookeeper_host", true, "hbase zookeeper host, default: hbase");
        options.addOption("hbase_zookeeper_port", true, "hbase zookeeper port, default: 2181");

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption("kafka_brokers")) {
            brokers = line.getOptionValue("kafka_brokers");
        }else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("flink write hbase job", options);
            System.exit(1);
        }
        if (line.hasOption("kafka_username")) {
            username = line.getOptionValue("kafka_username");
        }
        if (line.hasOption("kafka_user_password")) {
            password = line.getOptionValue("kafka_user_password");
        }
        if (line.hasOption("kafka_topic")) {
            topic = line.getOptionValue("kafka_topic");
        }
        if (line.hasOption("hbase_zookeeper_host")) {
            hbase_zookeeper_host = line.getOptionValue("hbase_zookeeper_host");
        }
        if (line.hasOption("hbase_zookeeper_port")) {
            hbase_zookeeper_port = line.getOptionValue("hbase_zookeeper_port");
        }

//        执行任务
        doExcute(brokers, username, password, topic, hbase_zookeeper_host, hbase_zookeeper_port);

    }
    public static void doExcute(String kafka_broker, String kafka_username, String kafka_password,
                                String topic, String hbase_zookeeper_host, String hbase_zookeeper_port) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        设置
        env.enableCheckpointing(5000 * 100000);

        Properties props = getKafkaProperties(kafka_username, kafka_password);
        props.setProperty("boostrap.server", kafka_broker);
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer(topic, new SimpleStringSchema(), props));
//        过滤不标准格式的数据，并格式化
        DataStream<HttpDataModel> formated_stream = stream.filter(s -> {
            JSONObject obj = JSONObject.parseObject(s);
            return obj.containsKey("project") && obj.containsKey("table") && obj.containsKey("data");
        }).map(s -> {return JSON.parseObject(s, HttpDataModel.class);});

//    在10s的时间窗口内，每100条触发输出到hbase
        DataStream<List<HttpDataModel>> batch_stream = formated_stream.timeWindowAll(Time.seconds(10)).trigger(new CountTrigger(100))
                .apply(new AllWindowFunction<HttpDataModel, List<HttpDataModel>, Window>() {
                    @Override
                    public void apply(Window window, Iterable<HttpDataModel> values, Collector<List<HttpDataModel>> out) throws Exception {
                        List<HttpDataModel> lists = new ArrayList<HttpDataModel>();
                        for (HttpDataModel value: values) {
                            lists.add(value);
                        }
                        out.collect(lists);
                    }
                });
        batch_stream.addSink(new HbaseSink(hbase_zookeeper_host, hbase_zookeeper_port));

//        控制台输出
//        batch_stream.print()
        env.execute("integration-http");
    }
    public static Properties getKafkaProperties(String username, String password) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("group.id", "dataworks-integration");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "10000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserialize", "org.apache.kafka.common.serialization.StringDeserializer");
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", jaasCfg);
        return props;
    }
}
