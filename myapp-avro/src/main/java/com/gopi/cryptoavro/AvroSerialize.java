package com.gopi.cryptoavro;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.security.KeyPairGenerator;
import java.util.Base64 ;;
import javax.crypto.Cipher ;
import java.lang.Exception ;
import java.security.Key ;
import java.security.KeyPair ;

public class AvroSerialize {;

	static int RSA_KEY_LENGTH = 4096;
	static String ALGORITHM_NAME = "RSA" ;
	static String PADDING_SCHEME = "OAEPWITHSHA-512ANDMGF1PADDING" ;
	static String MODE_OF_OPERATION = "ECB" ; // This essentially means none behind the scene

	public static void main(String args[]) throws IOException {

	    try {

            // Generate Key Pairs
            KeyPairGenerator rsaKeyGen = KeyPairGenerator.getInstance(ALGORITHM_NAME);
            rsaKeyGen.initialize(RSA_KEY_LENGTH);
            KeyPair rsaKeyPair = rsaKeyGen.generateKeyPair();

            // Instantiating the Schema.Parser class.
            Schema schema = new Schema.Parser().parse(new File("/home/user/myapp-avro/avro/emp.avsc"));

            // Instantiating the GenericRecord class.
            GenericRecord e1 = new GenericData.Record(schema);

            String name1 = "ramu";
            int id1 = 001;
            int salary1 = 200000;
            int age1 = 25;
            String address1 = "Chennai";

            String name1_en = rsaEncrypt(name1, rsaKeyPair.getPublic());
            int id1_en = Integer.valueOf(rsaEncrypt(String.valueOf(id1), rsaKeyPair.getPublic()));
            int salary1_en = Integer.valueOf(rsaEncrypt(String.valueOf(salary1), rsaKeyPair.getPublic()));
            int age1_en = Integer.valueOf(rsaEncrypt(String.valueOf(age1), rsaKeyPair.getPublic()));
            String address1_en = rsaEncrypt(address1, rsaKeyPair.getPublic());

            // Insert data according to schema
            e1.put("name", name1_en);
            e1.put("id", id1_en);
            e1.put("salary", salary1_en);
            e1.put("age", age1_en);
            e1.put("address", "address1_en");

            GenericRecord e2 = new GenericData.Record(schema);

            String name2 = "rahman";
            int id2 = 002;
            int salary2 = 300000;
            int age2 = 30;
            String address2 = "Mumbai";

            String name2_en = rsaEncrypt(name2, rsaKeyPair.getPublic());
            int id2_en = Integer.valueOf(rsaEncrypt(String.valueOf(id2), rsaKeyPair.getPublic()));
            int salary2_en = Integer.valueOf(rsaEncrypt(String.valueOf(salary2), rsaKeyPair.getPublic()));
            int age2_en = Integer.valueOf(rsaEncrypt(String.valueOf(age2), rsaKeyPair.getPublic()));
            String address2_en = rsaEncrypt(address2, rsaKeyPair.getPublic());


            e2.put("name", "name2_en");
            e2.put("id", id2_en);
            e2.put("salary", salary2_en);
            e2.put("age", age2_en);
            e2.put("address", "address2_en");

            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(schema, new File("/home/user/myapp-avro/avro/emp.txt"));

            dataFileWriter.append(e1);
            dataFileWriter.append(e2);
            dataFileWriter.close();
        }
        catch(NumberFormatException nfe) {System.out.println("Exception while encryption/decryption") ;
            nfe.printStackTrace() ; }

        catch(Exception e) {System.out.println("Exception while encryption/decryption") ;e.printStackTrace() ; }

		System.out.println("data successfully serialized");
	}



	public static String rsaEncrypt(String message, Key publicKey) throws Exception {

		Cipher c = Cipher.getInstance(ALGORITHM_NAME + "/" + MODE_OF_OPERATION + "/" + PADDING_SCHEME) ;

		c.init(Cipher.ENCRYPT_MODE, publicKey) ;

		byte[] cipherTextArray = c.doFinal(message.getBytes()) ;

		return Base64.getEncoder().encodeToString(cipherTextArray) ;

	}


	public static String rsaDecrypt(byte[] encryptedMessage, Key privateKey) throws Exception {
		Cipher c = Cipher.getInstance(ALGORITHM_NAME + "/" + MODE_OF_OPERATION + "/" + PADDING_SCHEME) ;
		c.init(Cipher.DECRYPT_MODE, privateKey);
		byte[] plainText = c.doFinal(encryptedMessage);

		return new String(plainText) ;

	}
}
