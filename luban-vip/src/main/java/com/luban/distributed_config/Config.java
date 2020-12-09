package com.luban.distributed_config;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Config {
    private static final String CONFIG_PREFIX = "/CONFIG";

    private ZooKeeper client;
    private Map<String, String> cache = new HashMap<>();

    public Config(String address) throws IOException {
        client = new ZooKeeper("127.0.0.1:2181", 1000 * 1000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("连接成功");
            }
        });

        init();
    }

    public void init() {
        try {
            List<String> childrenNames = client.getChildren(CONFIG_PREFIX, false);
            for (String name : childrenNames) {
                String value = new String(client.getData(getConfigFullName(name), false, new Stat()));
                cache.put(name, value);
            }

            // 监听CONFIG_PREFIX路径下的节点变化，
            client.addWatch(CONFIG_PREFIX, new Watcher() {
                @Override
                public void process(WatchedEvent event){

                    String path = event.getPath();
                    System.out.println(path);

                    if (path.startsWith(CONFIG_PREFIX)) {
                        String key = path.replace(CONFIG_PREFIX + "/", "");
                        if (Event.EventType.NodeCreated.equals(event.getType()) ||
                            Event.EventType.NodeDataChanged.equals(event.getType())) {
                            // 子节点创建 或 子节点修改

                            String value = null;
                            try {
                                value = new String(client.getData(path, false, new Stat()));
                            } catch (KeeperException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            cache.put(key, value);

                        } else if (Event.EventType.NodeDeleted.equals(event.getType())) {
                            // 子节点移除
                            cache.remove(key);
                        }
                    }
                }
            }, AddWatchMode.PERSISTENT_RECURSIVE);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    // 新增或更新配置，一个配置项对应一个zookeeper节点，节点内容为配置项值
    public void save(String name, String value) {
        try {
            String configFullName = getConfigFullName(name);
            Stat stat = client.exists(configFullName, false);
            if (stat != null) {
                // update
                client.setData(configFullName, value.getBytes(), -1);
            } else {
                // create
                client.create(configFullName, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            cache.put(name, value);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getConfigFullName(String name) {
        return CONFIG_PREFIX + "/" + name;
    }

    public String get(String name) {
        return cache.get(name);
    }
}
