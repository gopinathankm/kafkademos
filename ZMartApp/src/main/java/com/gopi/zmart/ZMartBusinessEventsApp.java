package com.gopi.zmart;

import com.gopi.zmart.clients.producer.MockDataProducer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 *  Author : Gopinathan Munappy
 *  Date : 16/11/2018
 *  Tume : 11.50 AM
 *
 */
public class ZMartBusinessEventsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartBusinessEventsApp.class);

    public static void main(String[] args) throws Exception {

        //Used only to produce data for this application, not typical usage
        MockDataProducer.produceRandomTextData();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ZMartBusinessEventsApp_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> simpleFirstStream = builder.stream("zmartin", Consumed.with(stringSerde, stringSerde));

        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues((ValueMapper<String, String>) String::toUpperCase);

        upperCasedStream.to( "zmartout", Produced.with(stringSerde, stringSerde));
        upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("ZMart Business Events App"));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        LOG.info("Hello World!,  ZMart Business Events AppStarted");
        kafkaStreams.start();
        Thread.sleep(10000);
        LOG.info("Shutting down the ZMart Business Events App now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}