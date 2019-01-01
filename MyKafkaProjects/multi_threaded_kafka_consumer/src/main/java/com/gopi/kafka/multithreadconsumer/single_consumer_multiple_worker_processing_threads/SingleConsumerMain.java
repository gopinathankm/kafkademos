package com.gopi.kafka.multithreadconsumer.single_consumer_multiple_worker_processing_threads;

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
 * Model #2: Single consumer, multiple worker processing threads
 *
 * Usage: user@user-Lenovo-G400:~/MyKafkaProjects/multi_threaded_kafka_consumer$ java -jar
 * multi_threaded_kafka_consumer_jar/multi_threaded_kafka_consumer.jar
 * com.gopi.kafka.multithreadconsumer.single_consumer_multiple_worker_processing_threads.SingleConsumerMain
 * localhost:9092 group1 multipleconsumers
 * Produces 5 messages
 * Sent:Message 0, Offset: 4
 * Sent:Message 1, Offset: 6
 * Sent:Message 2, Offset: 5
 * Sent:Message 3, Offset: 5
 * Sent:Message 4, Offset: 7
 *
 */
public final class SingleConsumerMain {


  public static void main(String[] args) {

    String brokers = "localhost:9092";
    String groupId = "group1";
    String topic = "multipleconsumers";
    int numberOfThread = 3;

    if (args != null && args.length > 4) {
      brokers = args[0];
      groupId = args[1];
      topic = args[2];
      numberOfThread = Integer.parseInt(args[3]);
    }

    // Start Notification Producer Thread
    NotificationProducerThread producerThread = new NotificationProducerThread(brokers, topic);
    Thread t1 = new Thread(producerThread);
    t1.start();

    // Start group of Notification Consumer Thread
    NotificationConsumer consumers = new NotificationConsumer(brokers, groupId, topic);

    consumers.execute(numberOfThread);

    try {
      Thread.sleep(100000);
    } catch (InterruptedException ie) {

    }
    consumers.shutdown();
  }
}
