package com.gopi.kafka.custompartitioner;

/**
 * @author Gopinathan Munappy
 * @class  KakfaCustomPartitionerMain
 *
 * Create a topic ourtopic as follows:
 * kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 1 --partitions 6 --topic
 */
public final class KakfaCustomPartitionerMain {

  public static void main(String[] args) {

    String brokers = "localhost:9092";
    String groupId = "ourgroup";
    String topic = "ourtopic";

    // Start User Producer Thread
    UserProducerThread producerThread = new UserProducerThread(brokers, topic);
    Thread t1 = new Thread(producerThread);
    t1.start();

    // Start group of User Consumer Thread
    UserConsumerThread consumerThread = new UserConsumerThread(brokers, groupId, topic);
    Thread t2 = new Thread(consumerThread);
    t2.start();

    try {
      Thread.sleep(100);
    } catch (InterruptedException ie) {

    }
  }
}
