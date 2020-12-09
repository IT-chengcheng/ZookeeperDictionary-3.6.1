package com.luban.distributed_lock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ZkLock implements Lock {

    private ThreadLocal<ZooKeeper> zk = new ThreadLocal<ZooKeeper>();
    private String LOCK_NAME = "/LOCK";

    private ThreadLocal<String> CURRENT_NODE = new ThreadLocal<String>();

    public void lock() {

        init();

        if (tryLock()) {
            //
            System.out.println("拿到锁了");
        }


    }

    private void init() {
        if (zk.get() == null) {
            try {
                zk.set(new ZooKeeper("localhost:2181", 3000, new Watcher() {
                    public void process(WatchedEvent watchedEvent) {
                        // ...
                    }
                }));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean tryLock() {

        String nodeName = LOCK_NAME + "/zk_";

        try {
            // 创建临时顺序节点  /LOCK/zk_1
            CURRENT_NODE.set(zk.get().create(nodeName, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));

            // 查子节点   // zk_1, zk_2, zk_3
            List<String> list = zk.get().getChildren(LOCK_NAME, false);
            Collections.sort(list);

            System.out.println(list);

            String minNode = list.get(0);

            if ((LOCK_NAME+ "/" + minNode).equals(CURRENT_NODE.get())) {
                return true;
            } else {
                // 等待锁
                // watch
                Integer currentIndex = list.indexOf(CURRENT_NODE.get().substring(CURRENT_NODE.get().lastIndexOf("/")+1));
                String prevNodeName = list.get(currentIndex-1);

                final CountDownLatch countDownLatch = new CountDownLatch(1);
                zk.get().exists(LOCK_NAME + "/" + prevNodeName, new Watcher() {
                    public void process(WatchedEvent watchedEvent) {
                        if (Event.EventType.NodeDeleted.equals(watchedEvent.getType())) {
                            countDownLatch.countDown();
                            System.out.println(Thread.currentThread().getName()+"唤醒锁了");
                        }
                    }
                });
                //..

                System.out.println(Thread.currentThread().getName()+"等待锁");
                countDownLatch.await();
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return false;
    }

    public void unlock() {

        try {
            zk.get().delete(CURRENT_NODE.get(), -1);
            CURRENT_NODE.remove();
            zk.get().close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }


    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public void lockInterruptibly() throws InterruptedException {

    }

    public Condition newCondition() {
        return null;
    }
}
