package com.example_kafka.multithrade;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadConsumer {

    public static void main(String... arg) {

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092"); // group de kafka
        props.setProperty("group.id", "des4j-group"); // identificador para el consumerGroup
        props.setProperty("enable.auto.commit", "true"); // de forma autonoma va ha estar realizando commit en proceso background a los registros o a los offset que se entán leyendo
        props.setProperty("auto.commit.interval.ms", "1000"); // frecuencia de commit a los offset
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // llave y tipo de dato de la llave
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // valor y tipo de dato del valor

        ExecutorService executor = Executors.newFixedThreadPool(5);

        for(int i = 0; i< 5; i++){
            ThreadConsumer consumer = new ThreadConsumer(new KafkaConsumer<>(props));
            executor.execute(consumer);
        }
        while(executor.isTerminated());

    }
}
