package com.luban;

public enum  EnumTest {

    A, B, C,

    D() {
        public void test() {
            System.out.println("2");
        }
    };

    public void test() {
        System.out.println("1");
    }

    public static void main(String[] args) {
        EnumTest.A.test();
    }



}
