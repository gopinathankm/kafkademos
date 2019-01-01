package com.gopi.partition;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public class TradeConsumer {

    public static void main(String[] args) {

        final String TOPIC = "trade2";
        String CONSUMER_GROUP_ID = "";

        if(args[0] == "")
            CONSUMER_GROUP_ID = "";
        else
            CONSUMER_GROUP_ID = args[0];

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(getConsumerProperties(CONSUMER_GROUP_ID));
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            consumerRecords.forEach(e -> {
                System.out.println(e.value());
            });
        }
    }

    private static Properties getConsumerProperties(String consumerGroupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", consumerGroupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }
}