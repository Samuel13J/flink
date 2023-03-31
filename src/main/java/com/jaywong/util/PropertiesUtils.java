package com.jaywong.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangjie
 * @create 2023-03-29 18:36
 */
public class PropertiesUtils {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtils.class);
    private static PropertiesUtils instance = null;
    private Integer flinkCheckpointInterval = null;
    private String flinkCheckpointMinPauseBetweenCheckpoints = null;
    private Integer flinkCheckpointCheckpointTimeout = null;
    private Integer flinkCheckpointMaxConcurrentCheckpoint = null;
    private Integer flinkFixedDelayRestartTimes = null;
    private Integer flinkFixedDelayRestartInterval = null;
    private String kafkaTopic = null;
    private String groupID = null;
    private String kafkaBootStrapServers = null;
    private boolean enableAutoCommit = Boolean.parseBoolean(null);
    private Integer maxPollRecords = null;
    private Integer autoCommitIntervalMS = null;
    private Integer sessionTimeoutMS = null;
    private Integer requestTimeoutMS = null;
    private String kafkaConsumerKeyDeserializer = null;
    private String kafkaConsumerValueDeserializer = null;
    private String mysqlUserName = null;
    private String mysqlPassword = null;
    private String mysqlDriverName = null;
    private String mysqldbURL = null;


    /**
     * 静态代码块
     */
    private PropertiesUtils() {
        InputStream in = null;
        try {
//            读取配置文件，通过类加载器的方式读取属性文件
            in = PropertiesUtils.class.getClassLoader().getResourceAsStream("constance.properties");
            Properties prop = new Properties();
            prop.load(in);
//            flink配置
            flinkCheckpointInterval = Integer.parseInt(prop.getProperty("flink.checkpoint.interval"));
            flinkCheckpointMinPauseBetweenCheckpoints = prop.getProperty("flink.checkpoint.minPauseBetweenCheckpoints");
            flinkCheckpointCheckpointTimeout = Integer.parseInt(prop.getProperty("flink.checkpoint.checkpointTimeout"));
            flinkCheckpointMaxConcurrentCheckpoint = Integer.parseInt(prop.getProperty("flink.checkpoint.maxConcurrentCheckpoint"));
            flinkFixedDelayRestartTimes = Integer.parseInt(prop.getProperty("flink.fixedDelayRestart.times"));
            flinkFixedDelayRestartInterval = Integer.parseInt(prop.getProperty("flink.fixedDelayRestart.interval"));
//            kafka配置
            kafkaTopic = prop.getProperty("kafka.topic");
            kafkaBootStrapServers = prop.getProperty("kafka.consumer.bootstrap.servers").trim();
            enableAutoCommit = Boolean.valueOf(prop.getProperty("kafka.consumer.enable.auto.commit"));
            maxPollRecords = Integer.parseInt(prop.getProperty("kafka.consumer.max.poll.records"));
            autoCommitIntervalMS = Integer.parseInt(prop.getProperty("kafka.consumer.auto.commit.interval.ms"));
            sessionTimeoutMS = Integer.parseInt(prop.getProperty("kafka.consumer.session.timeout.ms"));
            requestTimeoutMS = Integer.parseInt(prop.getProperty("kafka.consumer.request.timeout.ms"));
            groupID = prop.getProperty("kafka.consumer.group.id").trim();
            mysqlUserName = prop.getProperty("mysql.username").trim();
            mysqlPassword = prop.getProperty("mysql.password").trim();
            mysqldbURL = prop.getProperty("mysql.dburl").trim();
            mysqlDriverName = prop.getProperty("mysql.driver.name").trim();
            kafkaConsumerKeyDeserializer = prop.getProperty("kafka.consumer.key.deserializer").trim();
            kafkaConsumerValueDeserializer = prop.getProperty("kafka.consumer.value.deserializer").trim();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("流关闭失败");
            }
        }
    }
    public static PropertiesUtils getInstance() {
        if (instance == null) {
            instance = new PropertiesUtils();
        }
        return instance;
    }
    public String getKafkaTopic() {
        return kafkaTopic;
    }
    public String getkafkaBootStrapServers() {
        return kafkaBootStrapServers;
    }
    public boolean getenableAutoCommit() {
        return enableAutoCommit;
    }
    public Integer getmaxPollRecords() {
        return maxPollRecords;
    }
    public Integer getautoCommitIntervalMS() {
        return autoCommitIntervalMS;
    }
    public Integer getsessionTimeoutMS() {
        return sessionTimeoutMS;
    }
    public Integer getrequestTimeoutMS() {
        return requestTimeoutMS;
    }
    public String getgroupID() {
        return groupID;
    }
    public String getKafkaConsumerKeyDeserializer() {
        return kafkaConsumerKeyDeserializer;
    }
    public String getKafkaConsumerValueDeserializer() {
        return kafkaConsumerValueDeserializer;
    }
    public String getmysqlUserName() {
        return mysqlUserName;
    }
    public String getmysqlPassword() {
        return mysqlPassword;
    }
    public String getmysqldbURL() {
        return mysqldbURL;
    }
    public String getmysqlDriverName() {
        return mysqlDriverName;
    }
    public Integer getFlinkCheckpointInterval() {
        return flinkCheckpointInterval;
    }
    public String getFlinkCheckpointMinPauseBetweenCheckpoints() {
        return flinkCheckpointMinPauseBetweenCheckpoints;
    }
    public Integer getFlinkCheckpointCheckpointTimeout() {
        return flinkCheckpointCheckpointTimeout;
    }
    public Integer getFlinkCheckpointMaxConcurrentCheckpoint() {
        return flinkCheckpointMaxConcurrentCheckpoint;
    }
    public Integer getFlinkFixedDelayRestartTimes() {
        return flinkFixedDelayRestartTimes;
    }
    public Integer getFlinkFixedDelayRestartInterval() {
        return flinkFixedDelayRestartInterval;
    }
}
