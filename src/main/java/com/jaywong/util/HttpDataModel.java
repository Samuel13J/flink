package com.jaywong.util;


/**
 * @author wangjie
 * @create 2023-04-23 14:52
 */
public class HttpDataModel {
    private String project;
    private String table;
    private String data;
    private String brokers;
    private String username;
    private String password;
    private String topic;

    public HttpDataModel(String project, String table, String data) {
        this.project = project;
        this.table = table;
        this.data = data;
    }

    public String getProject() {
        return project;
    }

    public String getTable() {
        return table;
    }

    public String getData() {
        return data;
    }
    public String getFullTable() {
        return project + ":" + table;
    }
    public void setProject(String project) {
        this.project = project;
    }
    public void setTable(String table) {
        this.table = table;
    }
    public void setData(String data) {
        this.data = data;
    }

    public String getBrokers() {
        return brokers;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public String toString() {
        return "HttpDataModel(" +
                "project='" + project + '\'' +
                ", table='" + table + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
