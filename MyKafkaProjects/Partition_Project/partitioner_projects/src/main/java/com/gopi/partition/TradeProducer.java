package com.gopi.partition;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class TradeProducer {

    public static void main(String[] args) {
        final String TOPIC = "trade2";
        KafkaProducer kafkaProducer = new KafkaProducer(getProducerProperties());

        Runnable task1 = () -> sendTradeToTopic(TOPIC, kafkaProducer, "ABCD", 1, 5);
        Runnable task2 = () -> sendTradeToTopic(TOPIC, kafkaProducer, "EFGH", 6, 10);
        Runnable task3 = () -> sendTradeToTopic(TOPIC, kafkaProducer, "IJKL", 11, 15);

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.submit(task1);
        executorService.submit(task2);
        executorService.submit(task3);

        executorService.shutdown();

    }

    private static void sendTradeToTopic(String topic, KafkaProducer kafkaProducer, String securityId, int idStart, int idEnd) {
        String fundShortName = "Apple";
        String value = "$2000";
        for (int i = idStart; i <= idEnd; i++) {
            Trade trade = new Trade();
            trade.setId(Integer.toString(i));
            trade.setSecurityId(securityId);
            trade.setFundShortName("Apple");
            trade.setValue("$2000");

            try {
                String s = new ObjectMapper().writeValueAsString(trade);
                kafkaProducer.send(new ProducerRecord(topic, trade.getSecurityId(), s));
                System.out.println("Sending to " + topic + "msg : " + s);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        String KAFKA_SERVER_IP = "localhost:9092";
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_IP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}