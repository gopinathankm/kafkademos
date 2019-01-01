package com.gopi.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Set;

import static com.gopi.offset.KafkaConsumerUtil.TOPIC;
import static com.gopi.offset.KafkaConsumerUtil.createConsumer;

/**
 * Created by Gopinathan Munappy on 25.10.2018.
 */
public class KafkaConsumerFromOffset {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList(TOPIC));

        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (flag) {
                Set<TopicPartition> assignments = consumer.assignment();
                assignments.forEach(topicPartition ->
                        consumer.seek(
                                topicPartition,
                                90));
                flag = false;
            }

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }
}
