package com.example.kafkaconsumer.service;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ConsumerThread implements DisposableBean, Runnable {
    @Autowired
    private MyAutoCommitConsumer kafkaReader;

    private Thread thread;
    private volatile boolean someCondition = true;

    public ConsumerThread() {
        this.thread = new Thread(this);
        this.thread.start();
    }

    @Override
    public void run() {
        try {
            System.out.println("ConsumerThread...............");
            Thread.sleep(3000);
            kafkaReader.consumer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() throws Exception {
        someCondition = false;
    }
}
