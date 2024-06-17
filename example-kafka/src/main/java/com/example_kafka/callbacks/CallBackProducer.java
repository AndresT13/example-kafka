package com.example_kafka.callbacks;


import com.example_kafka.producers.ProducerKafka;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CallBackProducer {

    public static final Logger log = LoggerFactory.getLogger(CallBackProducer.class);

    public static void main(String... args) {

        long startTime = System.currentTimeMillis();

        //Producer de kafka con Java de forma Asincrona
        Properties props = new Properties();
        props.put("bootstra.servers", "localhost:9092"); // Broker de kafka al que nos vamos a conectar
        props.put("acks","1"); // props.put("acks","all");
        // formatos para enviar mensajes y los piuedo definir para enviar
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms","4");

        // try whit resource  Java solo invoca el método
        try(Producer<String, String> producer = new KafkaProducer<>(props);) {
            // se envian diez mil mensajes de forma asincrona y la respuesta también lo es no se espera en un orden secuencial
            for (int i = 0; i < 10000; i++) {
                producer.send(new ProducerRecord<String, String>("des4j-topic", "des4j-key", "des4j-value"), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            log.info("There was an error {} ", exception.getMessage());
                        }
                        log.info("Offset = {} , Partition= {} , Topic = {} ", metadata.offset(), metadata.partition(), metadata.topic());
                    }
                });
            }
            producer.flush();
        }
    }

}
