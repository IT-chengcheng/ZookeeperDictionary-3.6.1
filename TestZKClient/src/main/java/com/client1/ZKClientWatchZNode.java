package com.client1;

import java.io.IOException;
import java.security.PrivateKey;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 监听单节点内容
 */
public class ZKClientWatchZNode {

    private static  String connectString = "127.0.0.1:2181";
    //private static String connectString = "192.168.146.132:2181,192.168.146.133:2181,192.168.146.134:2181";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zkCli = new ZooKeeper(connectString, 3000,
                new Watcher() {
                    // 监听回调
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("监听回调  -> 监听路径为：" + event.getPath());
                        System.out.println("监听回调  -> 监听的类型为：" + event.getType());
                        System.out.println("监听回调  -> 监听被修改了！！！");
                    }
                });

        byte[] data = zkCli.getData("/tom", new Watcher() {
            // 监听的具体内容
            @Override
            public void process(WatchedEvent event) {
                System.out.println("监听路径为：" + event.getPath());
                System.out.println("监听的类型为：" + event.getType());
                System.out.println("监听被修改了！！！");
            }
        }, null);

        System.out.println(new String(data));
        Thread.sleep(Long.MAX_VALUE);
    }
}