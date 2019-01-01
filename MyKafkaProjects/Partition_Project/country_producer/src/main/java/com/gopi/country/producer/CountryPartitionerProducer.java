package com.gopi.country.producer;

import java.util.Properties;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.gopi.country.partitioner.CountryPartitioner;

/**
 * Created by Gopinathan Munappy on 27/11/2018
 *
 * CountryPartitionerProducer
 *
 * Runs in a dedicated thread. Contains core logic for producing event.
 *
 * Usage: java -jar ./country_producer_jar/country_producer.jar
 */
public class CountryPartitionerProducer implements Runnable {

    private static final String TOPIC_NAME = "country";
    private KafkaProducer<String, String> kafkaProducer = null;

    public CountryPartitionerProducer() {

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.ACKS_CONFIG, "0");

        configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CountryPartitioner.class.getCanonicalName());
        configProperties.put("partitions.0","USA");
        configProperties.put("partitions.1","India");

        kafkaProducer = new KafkaProducer(configProperties);

        } // constructor

    @Override
    public void run() {
        try {
            produce();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            }
        } // run

    /**
     * produce messages
     *
     * @throws Exception
     */
    private void produce() throws Exception {
        ProducerRecord<String, String> record = null;
        List<String> list = new ArrayList<String>();
        list.add("USA");
        list.add("India");
        list.add("France");

        //String random = list.get(new Random().nextInt(list.size()));

        try {
            Random rnd = new Random();
            while (true) {

                for (int i = 1; i <= 20; i++) {
                    String result = list.get(new Random().nextInt(list.size()));
                    result = result + ":  " + i + " order" ;
                    System.out.println(result);

                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(TOPIC_NAME, result);
                    kafkaProducer.send(rec, new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            System.out.println("Message sent to topic ->" + metadata.topic() + " , partition->" +
                            metadata.partition() + " stored at offset-> " +  metadata.offset());
                        }
                    });

                    /**
                     * wait before sending next message. this has been done on
                     * purpose
                     */
                    Thread.sleep(100);
                } // for
            } // while

        } // try
        catch (Exception e) {
            System.out.println("Producer thread was interrupted" + e.getMessage());

        } finally {
            kafkaProducer.close();
            System.out.println("Producer closed");
        } // finally

    }  // produce

} // class