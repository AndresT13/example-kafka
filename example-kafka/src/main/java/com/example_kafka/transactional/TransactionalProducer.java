package com.example_kafka.transactional;

import com.example_kafka.KafkaApplication;
import com.example_kafka.multithrade.ThreadConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TransactionalProducer {

    public static final Logger log = LoggerFactory.getLogger(ThreadConsumer.class);

    public static void main(String... arg) {
    long startTime = System.currentTimeMillis();
    //Producer de kafka con Java de forma Asincrona
    Properties props = new Properties();
		props.put("bootstra.servers", "localhost:9092"); // Broker de kafka al que nos vamos a conectar
		props.put("acks","all"); // props.put("acks","all"); dewbe ser all
        props.put("transactional.id","des4j-producer-id");
    // formatos para enviar mensajes y los piuedo definir para enviar
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms","4");

    // try whit resource  Java solo invoca el método
		try(
    Producer<String,String> producer = new KafkaProducer<>(props);) {
            try{
                producer.initTransactions();
                producer.beginTransaction();
                // se envian diez mil mensajes de forma asincrona y la respuesta también lo es no se espera en un orden secuencial
                for (int i = 0; i < 100000; i++) {
                    producer.send(new ProducerRecord<String, String>("des4j-topic", "des4j-key", "des4j-value"));
                    // simula un problema envio 50.000 registros y cuando llega a la mitad lanzó una excepción
                    // no realiza cdommit de la transacción y se va a abortar
                    if (i == 50000) {
                        throw new Exception("Unexpected Exception");
                    }
                }
                producer.commitTransaction();
                producer.flush();
            } catch(Exception e){
                log.error("Error ", e);
                producer.abortTransaction();
            }

    }
    // cuanto tiempo tardo en enviar los mensajes
		log.info("Processing time = {} ms",(System.currentTimeMillis() - startTime));

}


}
