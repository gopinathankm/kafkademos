
/**
 * @author Gopinathan Munappy  23/10/2018
 *
 * GBM Model to predict flight delays.
 *
 */

package com.gopi.java;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.BinomialModelPrediction;

import java.io.IOException;

// Program for prediction

public class PulsarConsumerPrediction {

    // Name of the generated H2O model
    private static String modelClassName = "com.gopi.java.gbm_pojo_test";

    // Prediction Value
    private static String airlineDelayPreduction = "unknown";
    private static String airline = " ";

    private static final Logger log = LoggerFactory.getLogger(PulsarConsumerPrediction.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC_NAME = "prediction_input";
    private static final String SUBSCRIPTION_NAME = "prediction-subscription";

    public static void main(String[] args) throws Exception {

        // Create H2O object (see gbm_pojo_test.java)
        hex.genmodel.GenModel rawModel = (hex.genmodel.GenModel) Class.forName(modelClassName).newInstance();
        EasyPredictModelWrapper model = new EasyPredictModelWrapper(rawModel);

        // Create a Pulsar client instance. A single instance can be shared across many
        // producers and consumer within the same application
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .build();

        // Here you get the chance to configure consumer specific settings. eg:
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(TOPIC_NAME)
                // Allow multiple consumers to attach to the same subscription
                // and get messages dispatched as a queue
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(SUBSCRIPTION_NAME)
                .subscribe();

        // Once the consumer is created, it can be used for the entire application lifecycle
        log.info("Created consumer for the topic {}", TOPIC_NAME);

        do {
            // Wait until a message is available
            Message<byte[]> msg = consumer.receive();
            // Extract the message as a printable string and then log
            String content = new String(msg.getData());
            log.info("Received message '{}' with ID {}", content, msg.getMessageId());
            // Acknowledge processing of the message so that it can be deleted
            consumer.acknowledge(msg);
        } while (true);
    }
}