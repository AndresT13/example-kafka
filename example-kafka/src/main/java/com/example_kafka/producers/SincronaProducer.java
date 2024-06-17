package com.example_kafka.producers;

import com.example_kafka.KafkaApplication;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SincronaProducer {

    public static final Logger log = LoggerFactory.getLogger(SincronaProducer.class);
    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);

        //Producer de kafka con Jave

        Properties props = new Properties();
        props.put("bootstra.servers", "localhost:9092"); // Broker de kafka al que nos vamos a conectar
        props.put("acks","1"); // props.put("acks","all");

        // formatos para enviar mensajes y los piuedo definir para enviar
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // try whit resource  Java solo invoca el método
        try(Producer<String,String> producer = new KafkaProducer<>(props);) {
            // se envian diez mil mensajes de forma asincrona y la respuesta también lo es no se espera en un orden secuencial
            for (int i = 0; i < 10000; i++) {
                // envío de mensajes de forma asincrona
                 producer.send(new ProducerRecord<String, String>("des4j-topic",String.valueOf(i), "des4j-value")).get();
            }
            producer.flush();
        } catch (InterruptedException | ExecutionException e) {
            log.error("MEssage producer interruped ", e);
        }

    }

}
