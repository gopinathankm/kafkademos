package com.gopi.kafka.multithreadconsumer.multiple_consumers_with_own_threads;

/**
 * Gopinathan Munappy
 *
 * Multi-threaded Apache Kafka consumer model
 *
 * There are 2 possible models I’d like to mention in this post.
 *
 *     Multiple consumers with their own threads (Model #1)
 *     Single consumer, multiple worker processing threads (Model #2)
 * * Both of them have their own pros and cons
 *  Model #1. Multiple consumers with their own threads
 *  Model #2. Single consumer, multiple worker processing threads
 *
 * Model #1. Multiple consumers with their own thread
 *
 * Usage: user@user-Lenovo-G400:~/MyKafkaProjects/multi_threaded_kafka_consumer$ java -jar
 * multi_threaded_kafka_consumer_jar/multi_threaded_kafka_consumer.jar2
 * com.gopi.kafka.multithreadconsumer.multiple_consumers_with_own_threads.MultipleConsumersMain
 * llocalhost:9092 group1 multipleconsumers
 */

public final class MultipleConsumersMain {

  public static void main(String[] args) {


    String brokers = "localhost:9092";
    String groupId = "group1";
    String topic = "multipleconsumers";
    int numberOfConsumer = 3;


    if (args != null && args.length > 4) {
      brokers = args[0];
      groupId = args[1];
      topic = args[2];
      numberOfConsumer = Integer.parseInt(args[3]);
    }

    // Start Notification Producer Thread
    NotificationProducerThread producerThread = new NotificationProducerThread(brokers, topic);
    Thread t1 = new Thread(producerThread);
    t1.start();

    // Start group of Notification Consumers
    NotificationConsumerGroup consumerGroup =
        new NotificationConsumerGroup(brokers, groupId, topic, numberOfConsumer);

    consumerGroup.execute();

    try {
      Thread.sleep(100000);
    } catch (InterruptedException ie) {

    }
  }
}
