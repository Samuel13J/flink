package com.jaywong.hbase;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;


/**
 * @author wangjie
 * @create 2023-04-23 11:29
 */
public class Hbase {
    public static void main(String[] args) throws Exception{
        String userKafkaTable = "" +
                "CREATE TABLE userKafkaTable (\n" +
                " uuid STRING, \n" +
                "pkg STRING, \n" +
                "channel STRING, \n" +
                "bdModel STRING, \n" +
                "rectime BIGINT \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka', \n" +
                " 'topic' = 'user',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup', \n" +
                " 'format' = 'json', \n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";

        String userDaylyKafkaTable = "" +
                "CREATE TABLE userDaylyKafkaTable (\n" +
                " uuid STRING, \n" +
                " pkg STRING, \n" +
                " channel STRING, \n" +
                " bdModel STRING, \n" +
                " rectime BIGINT \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka', \n" +
                " 'topic' = 'user', \n" +
                " 'properties.bootstrap.servers' = 'ukafka1:9092,ukafka2:9092,ukafka3:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'earliest-offset'\n" +
                ")";

        String userInfo = "" +
                "CREATE TABLE userInfo (\n" +
                " rowkey STRING,\n" +
                "f1 ROW<uuid STRING, pkg STRING, channel STRING, bdModel STRING, registerTime STRING, activeDates STRING>,\n" +
                "PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4', \n" +
                " 'table-name' = 'userInfo', \n" +
                " 'zookeeper.quorum' = 'localhost:2181' \n" +
                ")";

        String insertUserInfoSql = "" +
                "INSERT INTO userInfo\n" +
                "SELECT rowkey,ROW(uuid,pkg,channel,bdModel,registerTime,activeDates) FROM(\n" +
                "    SELECT t1.rowkey, t1.uuid,t1.pkg,t1.channel,t1.bdModel,COALESCE(t2.registerTime,CAST(t1.registerTime AS STRING)) registerTime,removeDuplicate(t1.activeDates,t2.activeDates) activeDates FROM (\n" +
                "        SELECT md5(uuid) rowkey,LAST_VALUE(uuid) uuid,LAST_VALUE(pkg) pkg,LAST_VALUE(channel) channel,LAST_VALUE(bdModel) bdModel,min(rectime) registerTime, mkString(FROM_UNIXTIME(rectime/1000, 'yyyy-MM-dd')) activeDates FROM\n" +
                "        (\n" +
                "            SELECT uuid,pkg,channel,bdModel,rectime FROM userKafkaTable \n" +
                "            UNION ALL \n" +
                "            SELECT uuid,pkg,channel,bdModel,rectime FROM userDaylyKafkaTable\n" +
                "        ) GROUP BY md5(uuid)\n" +
                "    ) t1 LEFT JOIN userInfo t2 ON(t1.rowkey=t2.rowkey)\n" +
                ")";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("mkString", new MkStringAggregateFunction());

    }

    public static class MkStringAggregateFunction extends AggregateFunction<String, HashMap<String, String>> {
        @Override
        public String getValue(HashMap<String, String> accMap) {
            String[] results = accMap.keySet().toArray(new String[0]);
            Arrays.sort(results);
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < results.length; i++) {
                sb.append(results[i]);
                if (i < results.length - 1){
                    sb.append(",");
                }
            }
            return sb.toString();
        }

        @Override
        public HashMap<String, String> createAccumulator() {
            return new HashMap<>();
        }
        public void accumulate(HashMap<String, String> accMap, String s){
            accMap.put(s, null);
        }
        public void retract(HashMap<String, String> accMap, String s){
            accMap.remove(s);
        }

        public void merge(HashMap<String, String> accMap, Iterable<HashMap<String, String>> it){
            Iterator<HashMap<String, String>> iter = it.iterator();
            while (iter.hasNext()) {
                accMap.putAll(iter.next());
            }
        }
        public void resetAccumulator(HashMap<String, String> accMap){
            accMap.clear();
        }

        public static class RemoveDuplicate extends ScalarFunction {
            public String eval(String s1, String s2) {
                if (s1 == null) {return s2;}
                if (s2 == null) {return s1;}

                String s3 = s1 + "," + s2;
                String[] results = s3.split(",");

                Arrays.sort(results);
                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < results.length; i++) {
                    sb.append(results[i]);
                    if (i < results.length - 1) {
                        sb.append(",");
                    }
                }
                return sb.toString();
            }
        }

    }
}
