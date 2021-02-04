package com.example.kafkaconsumer.config;

import org.springframework.beans.factory.annotation.Configurable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class HiveJDBCUtils  {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://172.16.53.235:10000/default";//端口默认10000
    private static String user = "root";
    private static String password = "Canghai4shenxiao$2020";

    public static Connection getConn() {
        return conn;
    }

    private static Connection conn = null;
    private static Statement stmt = null;

    // 加载驱动、创建连接
    public static Statement Connection() throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
        return stmt;
    }

}
