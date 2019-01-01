package com.gopi.country.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * Created by Gopinathan Munappy on 24/11/2018
 *
 * Using country topic and consumer group countrygroup
 *
 * Compile and create a fat JAR by invoking: mvn compile assembly:single.
 *
 * Create a topic named part-demo with three partitions and one replication factor:
 * <KAFKA_HOME>bin/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic country

 * Start three consumers, then watch the console to see how your partitions are assigned and revoked every time you start a new instance of the consumer:
 * java -jar ./country_consumer_jar/country_consumer.jar
 *
 * Start a producer:
 * java -jar ./country_producer_jar/country_producer.jar
 *
 */

public class CountryPartitionerConsumerApp {

    private static boolean stop = false;

    public static void main(String[] argv) throws Exception {

        String topicName = "country";
        String groupId = "countrygroup";

        ConsumerThread consumerThread = new ConsumerThread(topicName, groupId);
        consumerThread.start();

        consumerThread.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerThread.join();
    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName), new ConsumerRebalanceListener() {
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.printf("%s topic-partitions are revoked from this consumer\n",
                            Arrays.toString(partitions.toArray()));
                }
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.printf("%s topic-partitions are assigned to this consumer\n",
                            Arrays.toString(partitions.toArray()));
                }
            });

            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                        System.out.println(record.value());
                }
            } catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {

            return this.kafkaConsumer;
        }
    }
}