package com.luban.distributed_lock;

import java.util.concurrent.locks.Lock;

public class Main {


    public static void main(String[] args) {
        Thread thread1 = new Thread(new UserThread(), "user1");
        Thread thread2 = new Thread(new UserThread(), "user2");

        thread1.start();
        thread2.start();
    }

//    public static Lock lock = new ReentrantLock();
    public static Lock lock = new ZkLock();

    static class UserThread implements Runnable {
        public void run() {

            new Order().createOrder();
            lock.lock();
            Boolean result = new Stock().reduceStock();
            lock.unlock();
            if (result) {
                System.out.println(Thread.currentThread().getName() + "减库存成功");
                new Pay().pay();
            } else {
                System.out.println(Thread.currentThread().getName()+"减库存失败");
            }

        }
    }
}
