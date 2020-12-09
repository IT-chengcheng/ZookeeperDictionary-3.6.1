package com.luban;

import org.apache.zookeeper.server.persistence.Util;

import java.util.Locale;

public class Test {
    public static void main(String[] args) {
//        System.out.println(String.format(Locale.ENGLISH, "%010d", 1));
        System.out.println(Util.makeLogName(100));
    }
}
