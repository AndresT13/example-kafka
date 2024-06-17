package com.example_kafka.multithrade;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadConsumer extends Thread {

    private final KafkaConsumer<String, String> consumer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final Logger log = LoggerFactory.getLogger(ThreadConsumer.class);
    public ThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run(){
        try{
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            while(!closed.get()) {
                ConsumerRecords<String, String> consumerRecords= consumer.poll(Duration.ofMillis(10000));
                for(ConsumerRecord<String, String> consumerRecord :consumerRecords) {
                    log.debug("offset = {}, key = {}, value = {}" ,consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
                    if((Integer.parseInt(consumerRecord.key()))%100000==0){
                        log.info("offset = {}, key = {}, value = {}" ,consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
                    }
                }
            }
        }catch(WakeupException e) {
            if(!closed.get())
                throw e;
        }finally{
            consumer.close();
        }
    }
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

}
