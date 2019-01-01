package com.gopi.topic;

import com.gopi.topic.Consumer;
import com.gopi.topic.Producer;
import com.gopi.topic.TopicAdmin;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by Gopinathan Munappy on 29/11/2018
 *
 * Driver program for TopicAdmin module
 */

// Handle starting producer or consumer

public class TopicApp {

    public static void main(String[] args) throws IOException {

        if(args.length < 3) {
            usage();
        }
        // Get the brokers
        String brokers = args[2];
        String topicName = args[1];
        switch(args[0].toLowerCase()) {
            case "producer":
                Producer.produce(brokers, topicName);
                break;
            case "consumer":
                // Either a groupId was passed in, or we need a random one
                String groupId;
                if(args.length == 4) {
                    groupId = args[3];
                } else {
                    groupId = UUID.randomUUID().toString();
                }
                Consumer.consume(brokers, groupId, topicName);
                break;
            case "describe":
                TopicAdmin.describeTopics(brokers, topicName);
                break;
            case "create":
                TopicAdmin.createTopics(brokers, topicName);
                break;
            case "delete":
                TopicAdmin.deleteTopics(brokers, topicName);
                break;
            default:
                usage();
        }
        System.exit(0);
    }

    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("<producer|consumer|describe|create|delete> <topicName> brokerhosts [groupid]");
        System.exit(1);
    }
}