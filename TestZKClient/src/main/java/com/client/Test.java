package com.client;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.persistence.Util;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicLong;

public class Test {
    public static void main(String[] args) throws NoSuchAlgorithmException {
//        System.out.println(String.format(Locale.ENGLISH, "%010d", 1));
        System.out.println(Util.makeLogName(100));

        System.out.println(DigestAuthenticationProvider.generateDigest("super:admin"));
        System.out.println(DigestAuthenticationProvider.generateDigest("zhangsan:123456"));
    }
    private final AtomicLong nextExpirationTime = new AtomicLong();

    public void test() {
        System.out.println(nextExpirationTime.get());

        Integer a = 0;
        nextExpirationTime.compareAndSet(a, 1);

        System.out.println(a);
        System.out.println(nextExpirationTime.get());
    }
}
