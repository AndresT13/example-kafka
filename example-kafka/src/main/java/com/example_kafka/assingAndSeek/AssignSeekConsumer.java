package com.example_kafka.assingAndSeek;

import com.example_kafka.producers.ProducerKafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignSeekConsumer {

    public static final Logger log = LoggerFactory.getLogger(ProducerKafka.class);

    public static void main(String... arg) {

        Properties props = new Properties();

        props.setProperty("bootstrap.servers", "localhost:9092"); // group de kafka
        props.setProperty("group.id", "des4j-group"); // identificador para el consumerGroup
        props.setProperty("enable.auto.commit", "true"); // de forma autonoma va ha estar realizando commit en proceso background a los registros o a los offset que se entán leyendo
        props.setProperty("auto.offser.reset", "earliest"); // frecuencia de commit a los offset
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // llave y tipo de dato de la llave
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // valor y tipo de dato del valor

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);) {
            TopicPartition topicPartition = new TopicPartition("des4j.topic",4);
            consumer.assign(Arrays.asList(topicPartition));

            consumer.seek(topicPartition,50); // debe ir el topic y en donde quiero que empiece
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
