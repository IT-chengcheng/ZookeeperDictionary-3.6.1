package com.luban;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongTest {

    private final AtomicLong nextExpirationTime = new AtomicLong();

    public void test() {
        System.out.println(nextExpirationTime.get());

        Integer a = 0;
        nextExpirationTime.compareAndSet(a, 1);

        System.out.println(a);
        System.out.println(nextExpirationTime.get());
    }

    public static void main(String[] args) {
        new AtomicLongTest().test();
    }
}
