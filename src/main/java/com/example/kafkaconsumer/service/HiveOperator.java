package com.example.kafkaconsumer.service;

import com.example.kafkaconsumer.config.HiveJDBCUtils;
import java.sql.Connection;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.Statement;

@Service
public class HiveOperator {

    private  Connection conn = null;
    private  Statement stmt = null;
    private  ResultSet rs = null;

    public void run() throws Exception {
        HiveJDBCUtils h = new HiveJDBCUtils();
        stmt = h.Connection();
        conn = h.getConn();
        showDatabases();
        destory();
    }

    public  void showDatabases() throws Exception {
        String sql = "select * from sea.logs01";
        stmt.executeQuery(sql);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }

    public void destory() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
