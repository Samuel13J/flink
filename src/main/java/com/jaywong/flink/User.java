package com.jaywong.flink;

/**
 * @author wangjie
 * @create 2023-03-29 18:08
 */
public class User {
    final String name;
    final String gender;
    final String tel;
    final String age;

    public User(String name, String gender, String tel, String age) {
        this.name = name;
        this.gender = gender;
        this.tel = tel;
        this.age = age;
    }
}
