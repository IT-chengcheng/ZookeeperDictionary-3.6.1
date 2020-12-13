package com.rpc;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class RPCServer {
    private String connectString = "127.0.0.1:2181";
    //private String connectString = "192.168.146.132:2181,192.168.146.133:2181,192.168.146.134:2181";
    private int sessionTimeout = 3000;
    ZooKeeper zkCli = null;
    // 定义父节点
    private String parentNode = "/servers";
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 1.连接zkServer
        RPCServer rpcServer = new RPCServer();
        rpcServer.getConnect();

        // 2.注册节点信息 服务器ip添加到zk中,如果电脑有多个网卡，这个方法拿到的IP地址不对，详见笔记
        rpcServer.regist(InetAddress.getLocalHost().getHostAddress());

        // 3.业务逻辑处理
        rpcServer.build(InetAddress.getLocalHost().getHostAddress());
    }



    // 1.连接zkServer
    public void getConnect() throws IOException {
        zkCli = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("rpc-server 连接zookeeper成功");
            }
        });
    }

    // 2.注册信息
    public void regist(String hostname) throws KeeperException, InterruptedException {
        String node = zkCli.create(parentNode + "/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("成功创建服务节点："+node);
    }

    // 3.构造服务器
    public void build(String hostname) throws InterruptedException {
        System.out.println(hostname + ":服务器上线了！");
        Thread.sleep(Long.MAX_VALUE);
    }
}