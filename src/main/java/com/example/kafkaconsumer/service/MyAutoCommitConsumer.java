package com.example.kafkaconsumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

@Service
public class MyAutoCommitConsumer {

    @Autowired
    private HiveOperator hiveOperator;

    private static final String filename = "/root/soft/book1.csv";

    public void consumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.53.235:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ogg_test")); //(Arrays.asList(tp));
        while (true) {
            System.out.println("************************* poll");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            System.out.println("*************************Length = " + records.count());
            if (records.count() > 0) {
                try {
                    consume(records);
                    Thread.sleep(2000);
                    CommandExec.run("/root/soft/hive.shell");
                    Thread.sleep(2000);
                    hiveOperator.run();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void consume(ConsumerRecords<String, String> records) throws IOException {
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            FileWriter writer = null;
            try {
                String json = record.value();
                System.out.println(json);
                ObjectMapper mapper = new ObjectMapper();
                HashMap map = mapper.readValue(json, HashMap.class);
                HashMap mapAfter = (HashMap) map.get("after");
                String id = mapAfter.get("MR_SECT_NO").toString();
                String msg = mapAfter.get("NAME").toString();
                String content = id + "," + msg;
                System.out.println(content);
                //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
                writer = new FileWriter(filename, true);
                writer.write(content);
                writer.write("\r\n");
            } finally {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
