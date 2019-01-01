package com.gopi.country.producer;

import java.util.logging.Logger;

/**
 *  Created by Gopinathan Munappy on 27/11/2018
 *
 * CountryPartitionerProducerApp
 *
 *  Entry point for the application. Kicks off Kafka Producer thread
 *
 *  Runs in a dedicated thread. Contains core logic for producing event.
 *
 *  Usage: java -jar ./country_producer_jar/country_producer.jar
 */

public class CountryPartitionerProducerApp {

    private static final Logger LOGGER = Logger.getLogger(CountryPartitionerProducerApp.class.getName());

    public static void main(String[] args) throws Exception {

        new Thread( new CountryPartitionerProducer() ).start();
        LOGGER.info("country metrics production triggered");

    }
}