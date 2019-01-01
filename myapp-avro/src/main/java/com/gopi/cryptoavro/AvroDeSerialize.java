package com.gopi.cryptoavro;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroDeSerialize {
    public static void main(String args[]) throws Exception {
        // Instantiating the Schema.Parser class.
        Schema schema = new Schema.Parser().parse(new File(
                "/home/user/myapp-avro/avro/emp.avsc"));
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
                schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
                new File("/home/user/myapp-avro/avro/emp.txt"),
                datumReader);
        GenericRecord emp = null;
        while (dataFileReader.hasNext()) {
            emp = dataFileReader.next(emp);
            System.out.println(emp);
        }
        System.out.println("data deserialized!");
    }
}