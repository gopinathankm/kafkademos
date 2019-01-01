package com.gopi.offset;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Scanner;

/**
 * @author Gopinathan Munappy
 *
 * @date    01/12/2018
 *
 * Usage 1: Run Producer mytopic
 * Hello Kafka
 * Message sent to topic ->mytopic stored at offset->0
 * Again Hello Kafka
 * Message sent to topic ->mytopic stored at offset->1
 * One more Hello Kafka
 * Message sent to topic ->mytopic stored at offset->2
 * exit
 *
 * Usage 2 Run Producer group-test
 * message 1
 * Message sent to topic ->group-test stored at offset->0
 * message 2
 * Message sent to topic ->group-test stored at offset->0
 * message 3
 * Message sent to topic ->group-test stored at offset->1
 * message 4
 * Message sent to topic ->group-test stored at offset->1
 * message 5
 * Message sent to topic ->group-test stored at offset->2
 */
public class Producer {

    private static Scanner in;
    public static void main(String[] argv)throws Exception {
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        String topicName = argv[0];
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        String line = in.nextLine();
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, null, line);
            producer.send(rec, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("Message sent to topic ->" + metadata.topic() +" stored at offset->" + metadata.offset());
                }
            });
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }
}