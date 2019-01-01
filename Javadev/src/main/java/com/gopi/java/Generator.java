package com.gopi.java;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * Created by Gopinathan K. Munappy on  23/10/2018.
 */

public class Generator {

    public static void main(String[] argv) throws Exception {

        Generator generator = new Generator();
        String fileToRead   = "/home/user/data/1987.csv";
        generator.generate(fileToRead);
        } // main

    // method for generating airline new data as a stream

    public static void generate(String file) {

        String line = null;
        // String fileToRead="";
        FileReader filereader = null;
        BufferedReader br = null;

        try {
            filereader = new FileReader(file);
            br = new BufferedReader(filereader);

            int i =0;
            // we are going to read data line by line
            while ((line = br.readLine()) != null) {
                if(i == 0) {
                    i++;
                    continue;
                }
                System.out.println(line);
                Thread.sleep(5000);

            } // while
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        finally{
            try {
                filereader.close();
                br.close();
            }
            catch(IOException ioe) {
                ioe.printStackTrace();
            }
        }

      }   // generate()

} // class