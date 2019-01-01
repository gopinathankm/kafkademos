package com.gopi.kafka.study;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;

public class MyProducer {

    public static void main(String[] argv)throws Exception {
        String topicName = "mytopic2";

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);
        List<String> list = new ArrayList<String>();
        list.add("USA");
        list.add("India");
        list.add("France");

        TestCallback callback = new TestCallback();
        ProducerRecord<String, String> record = null;

              for (int i = 1; i <= 20; i++) {
                  String key = list.get(new Random().nextInt(list.size()));

                  String value = "order: " + i;
                  String message = key + "," + value;
                  System.out.println(key);
                  System.out.println(value);
                  System.out.println(message);
                  record = new ProducerRecord<String, String>(topicName, key, message);
                  // System.out.println("Key :" + record.key() + "Value :" + record.value() + "Message :" + message);
                  producer.send(record, callback);

                  Thread.sleep(1000);
            } // for
            producer.close();

    } //main


        private static class TestCallback implements Callback {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    System.out.println("Error while producing message to topic :" + recordMetadata);
                    e.printStackTrace();
                } else {
                    String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                    System.out.println(message);
                }
            }
        }
}