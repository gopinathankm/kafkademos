package com.gopi.zmart.producer;

import com.gopi.zmart.model.Purchase;
import com.gopi.zmart.partitioner.PurchaseKeyPartitioner;
import com.gopi.zmart.model.PurchaseKey;
import com.gopi.zmart.util.serializer.JsonSerializer;
import com.gopi.zmart.util.serializer.JsonDeserializer;

import com.gopi.zmart.util.serde.StreamsSerdes;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * User: Gopinathan Munappy
 * Date: 16//11/2018
 * Time: 9:09 AM
 * Example of a simple producer, not meant to run as a stand alone example.
 *
 * If desired to run this example change the ProducerRecord below to
 * use a real topic name and comment out line #33 below.
 */
public class SimpleProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "com.gopi.zmart.util.serializer.JsonSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("compression.type", "snappy");
        // This line is for demonstration purposes
        properties.put("partitioner.class", PurchaseKeyPartitioner.class.getName());

        PurchaseKey key = new PurchaseKey("12334568", new Date());

        // Creates the Serializer for the Purchase class
        JsonSerializer<PurchaseKey> purchaseKeyJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<PurchaseKey> purchaseKeyJsonDeserializer = new JsonDeserializer<>(PurchaseKey.class);
        Serde<PurchaseKey> purchaseKeySerde = Serdes.serdeFrom(purchaseKeyJsonSerializer, purchaseKeyJsonDeserializer);

        Serde<String> stringSerde = Serdes.String();

        try(Producer<PurchaseKey, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<PurchaseKey, String> record = new ProducerRecord<>("some-topic", key, "simple value");

            System.out.println("simple value");

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Encountered exception " + exception);
                    exception.printStackTrace();
                }
            };

            Future<RecordMetadata> sendFuture = producer.send(record, callback);
        }

    }


}