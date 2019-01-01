package com.gopi.offset;

/**
 * Gopinathan Munappy 27/10/2018
 * OffsetRewindApp
 *
 */
public class OffsetRewindApp
{
    public static void main( String[] args ) {

        OffsetRewindProducer orp = new OffsetRewindProducer();
        orp.produce();
    } // main
}