package com.gopi.kafka.serialization.custom;

/**
 * Gopinathan Munappy
 * KakfaCustomSerializerMain
 *
 * user@user-Lenovo-G400:~/MyKafkaProjects/kafka_custom_serialization$ java -jar kafka_custom_serialization_jar/kafka_custom_serialization.jar
        Sent:User [id=1, userName=tom, firstName=Tom, lastName=Riddle, age=40]
        Receive: User [id=1, userName=tom, firstName=Tom, lastName=Riddle, age=40]
        Sent:User [id=2, userName=harry, firstName=Harry, lastName=Potter, age=10]
 */

public final class KakfaCustomSerializerMain {

  public static void main(String[] args) {

    String brokers = "localhost:9092";
    String groupId = "group01";
    String topic = "usermessage";

   /** if (args != null && args.length == 3) {
      brokers = args[0];
      groupId = args[1];
      topic = args[2];
    }
    */

    // Start User Producer Thread
    UserProducerThread producerThread = new UserProducerThread(brokers, topic);
    Thread t1 = new Thread(producerThread);
    t1.start();

    // Start group of User Consumer Thread
    UserConsumerThread consumerThread = new UserConsumerThread(brokers, groupId, topic);
    Thread t2 = new Thread(consumerThread);
    t2.start();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException ie) {

    }
  }
}
