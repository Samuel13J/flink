package com.jaywong.sink;

import com.jaywong.util.HttpDataModel;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author wangjie
 * @create 2023-04-23 14:56
 */
public class HbaseSink extends RichSinkFunction<List<HttpDataModel>> implements Serializable {
    private Logger log;
    private String hbase_zookeeper_host;
    private String Hbase_zookeeper_port;

    private Connection connection;
    private Admin admin;

    public HbaseSink(String hbase_zookeeper_host, String hbase_zookeeper_port) {
        this.hbase_zookeeper_host = hbase_zookeeper_host;
        this.Hbase_zookeeper_port = hbase_zookeeper_port;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log = Logger.getLogger(String.valueOf(HbaseSink.class));
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", Hbase_zookeeper_port);
        configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_host);

        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
    }
    @Override
    public void invoke(List<HttpDataModel> datas, Context context) throws Exception {
//        按project:table归纳
        Map<String, List<HttpDataModel>> map = new HashMap<String, List<HttpDataModel>>();
        for (HttpDataModel data: datas) {
            if (!map.containsKey(data.getFullTable())) {
                map.put(data.getFullTable(), new ArrayList<HttpDataModel>());
            }
            map.get(data.getFullTable()).add(data);
        }
//            遍历 map
        for (Map.Entry<String, List<HttpDataModel>> entry : map.entrySet()) {
//            如果 表不存在，即创建
            createTable(entry.getKey());
//            写数据
            List<Put> list = new ArrayList<Put>();
            for (HttpDataModel item: entry.getValue()) {
                Put put = new Put(Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                JSONObject object = JSONObject.parseObject(item.getData());
                for (String key: object.keySet()) {
                    put.addColumn("data".getBytes(), key.getBytes(), object.getString(key).getBytes());
                }
                list.add(put);
            }
            connection.getTable(TableName.valueOf(entry.getKey())).put(list);
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
    }
    /**
     * 创建 hbase表
     */
    private void createTable(String tableName) throws Exception {
        createNamespace(tableName.split(":")[0]);
        TableName table = TableName.valueOf(tableName);
        if (! admin.tableExists(table)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
//            固定只有data列簇
            hTableDescriptor.addFamily(new HColumnDescriptor("data"));
            admin.createTable(hTableDescriptor);
        }
    }
    /**
     * 创建命名空间
     */
    private void createNamespace(String namespace) throws Exception {
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }

    }
}
