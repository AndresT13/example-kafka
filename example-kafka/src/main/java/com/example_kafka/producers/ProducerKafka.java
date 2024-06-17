package com.example_kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.util.Properties;

public class ProducerKafka<S, S1> {
    public static final Logger log = LoggerFactory.getLogger(ProducerKafka.class);

    public static void main(String... args) {
        SpringApplication.run(ProducerKafka.class, args);
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
		try(
                Producer<String,String> producer = new KafkaProducer<>(props);) {
        // se envian diez mil mensajes de forma asincrona y la respuesta también lo es no se espera en un orden secuencial
        for (int i = 0; i < 10000; i++) {
            producer.send(new ProducerRecord<String, String>("des4j-topic", (i % 2 ==0 )? "key-2.1":"key-3.1", String.valueOf(i)));
        }
        producer.flush();
    }
    // cuanto tiempo tardo en enviar los mensajes
		log.info("Processing time = {} ms",(System.currentTimeMillis() - startTime));

}
}
