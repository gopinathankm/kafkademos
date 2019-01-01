package com.gopi.kafka.clients;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Gopinathan Munappy
 *
 * Producer program
 *
 */
public class KafkaClients_Producer {

    public static void main( String[] args ) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "clientstopic";
        Producer<String, String> producer = null;

        try {
            producer = new KafkaProducer<>(props);
            for (int i = 0; i < 100; i++) {
                String message = "Message " + i;
                producer.send(new ProducerRecord<String, String>(topic, message));
                System.out.println("Sent:" + message + "to topic: " + topic);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }

    }

}