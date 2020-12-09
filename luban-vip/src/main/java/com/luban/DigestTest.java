package com.luban;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.security.NoSuchAlgorithmException;

public class DigestTest {

    public static void main(String[] args) throws NoSuchAlgorithmException {
        System.out.println(DigestAuthenticationProvider.generateDigest("super:admin"));
        System.out.println(DigestAuthenticationProvider.generateDigest("zhangsan:123456"));
    }
}
