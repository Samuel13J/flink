package com.jaywong.mysql;

/**
 * @author wangjie
 * @create 2023-03-29 18:25
 */

import akka.japi.tuple.Tuple4;
import com.jaywong.util.PropertiesUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlImpl extends RichSinkFunction<Tuple4<String, Integer, String, String>> implements Serializable {
//    private static final long serialVersionUID = 1L;
/**
 *     PropertiesUtils instance = PropertiesUtils.getInstance();
 *     出现了以下报错 包含有不可序列化文件，需要用 private static final 修饰
 *     com.jaywong.util.PropertiesUtils@3e2fc448 is not serializable.
 *     The object probably contains or references non serializable fields.
 */
    private static final PropertiesUtils instance = PropertiesUtils.getInstance();
    private Connection connection;
    private PreparedStatement preparedStatement;


    @Override
    public void invoke(Tuple4<String, Integer, String, String> value, Context context) throws Exception {
        //SinkFunction.super.invoke(value, context);
        Class.forName(instance.getmysqlDriverName());
        connection = DriverManager.getConnection(instance.getmysqldbURL(), instance.getmysqlUserName(),
                instance.getmysqlPassword());
        String sql = "insert into brtl_pd_rtl (name ,age,sex,tel) values(?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.t1());
        preparedStatement.setInt(2, value.t2());
        preparedStatement.setString(3, value.t3());
        preparedStatement.setString(4, value.t4());
        preparedStatement.execute();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
