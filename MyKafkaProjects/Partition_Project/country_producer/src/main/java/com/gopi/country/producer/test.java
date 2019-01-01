package com.gopi.country.producer;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class test {

    public static void main(String args[]) {

        List<String> list = new ArrayList<String>();
        list.add("1");
        list.add("2");
        list.add("3");
        for (int i = 1; i <= 20; i++) {
            String random = list.get(new Random().nextInt(list.size()));
            System.out.println(random);
        }

    }
}
