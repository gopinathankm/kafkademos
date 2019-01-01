package com.gopi.pulsar;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.IntStream;

public class PulsarProducer {
    private static final Logger log = LoggerFactory.getLogger(PulsarProducer.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "my-topic";

    public static void main(String[] args) throws IOException {
        // Create a Pulsar client instance. A single instance can be shared across many
        // producers and consumer within the same application
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();

        // Here you get the chance to configure producer specific settings
        Producer<byte[]> producer = client.newProducer()
                // Set the topic
                .topic(TOPIC_NAME)
                // Enable compression
                .compressionType(CompressionType.LZ4)
                .create();

        // Once the producer is created, it can be used for the entire application life-cycle
        log.info("Created producer for the topic {}", TOPIC_NAME);

        // Send 10 test messages
        IntStream.range(1, 11).forEach(i -> {
            String content = String.format("hello-pulsar-%d", i);

            // Build a message object
            Message<byte[]> msg = MessageBuilder.create()
                    .setContent(content.getBytes())
                    .build();

            // Send each message and log message content and ID when successfully received
            try {
                MessageId msgId = producer.send(msg);

                log.info("Published message '{}' with the ID {}", content, msgId);
            } catch (PulsarClientException e) {
                log.error(e.getMessage());
            }
        });

        client.close();
    }
}

