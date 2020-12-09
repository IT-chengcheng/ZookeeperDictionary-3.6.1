package com.luban;

import java.nio.ByteBuffer;

public class ByteBufferTest {

    public static void main(String[] args) {
        ByteBuffer buf = ByteBuffer.allocate ( 1024 ) ;
        System.out.println(buf.remaining());
    }
}
