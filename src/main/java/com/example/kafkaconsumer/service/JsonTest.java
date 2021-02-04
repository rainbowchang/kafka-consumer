package com.example.kafkaconsumer.service;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

public class JsonTest {
    private static final String content = "partition = 4,offset = 0, key = null, value = {\"table\":\"SEA.R_SECT\",\"op_type\":\"I\",\"op_ts\":\"2021-02-04 09:34:02.010168\",\"current_ts\":\"2021-02-04T17:34:09.140000\",\"pos\":\"00000000010000001933\",\"after\":{\"MR_SECT_NO\":\"7\",\"NAME\":\"7\",\"ORG_NO\":\"7\",\"ATTR\":\"7\",\"GROUP_NO\":\"7\",\"GROUP_REMARK\":\"7\",\"EFFECT_FLAG\":\"7\",\"STATUS_DATE\":null}}";

    public static void main(String[] args){
        int head = content.indexOf("value");
        int tail = content.length();
        String json = content.substring(head+8, tail);
        System.out.println(json);


        ObjectMapper mapper=new ObjectMapper();
        try {
            HashMap map = mapper.readValue(json, HashMap.class);
            HashMap mapAfter = (HashMap)map.get("after");
            String id = mapAfter.get("MR_SECT_NO").toString();
            String msg = mapAfter.get("NAME").toString();
            System.out.println(id + "," + msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
