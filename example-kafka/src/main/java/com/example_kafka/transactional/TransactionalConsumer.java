package com.example_kafka.transactional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TransactionalConsumer {
    public static final Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);

    public static void main(String...  args){

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092"); // group de kafka
        props.setProperty("group.id", "des4j-group"); // identificador para el consumerGroup
        props.setProperty("enable.auto.commit", "true"); // de forma autonoma va ha estar realizando commit en proceso background a los registros o a los offset que se entán leyendo
        props.setProperty("isolation.level", "read_committed");
        props.setProperty("auto.commit.interval.ms", "1000"); // frecuencia de commit a los offset
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // llave y tipo de dato de la llave
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // valor y tipo de dato del valor

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);) {
            consumer.subscribe(Arrays.asList("des4j-topic"));
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String,
                        String> record : records)
                    log.info("partition = {} , offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
            }
        }

    }
}
