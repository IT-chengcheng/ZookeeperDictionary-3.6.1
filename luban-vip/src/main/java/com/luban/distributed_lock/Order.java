package com.luban.distributed_lock;

public class Order {

    public void createOrder() {
        System.out.println(Thread.currentThread().getName() + "创建order");
    }
}
