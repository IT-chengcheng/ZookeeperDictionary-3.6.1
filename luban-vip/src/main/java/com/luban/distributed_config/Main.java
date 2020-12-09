package com.luban.distributed_config;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * *************书山有路勤为径***************
 * 鲁班学院
 * 往期资料加木兰老师  QQ: 2746251334
 * VIP课程加安其拉老师 QQ: 3164703201
 * 讲师：周瑜老师
 * *************学海无涯苦作舟***************
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        Config config = null;
        try {
            config = new Config("localhost:2181");
        } catch (IOException e) {
            e.printStackTrace();
        }

        config.save("timeout", "1");
        for (int i=0; i<100; i++) {
            System.out.println("====="+config.get("timeout"));
            System.out.println("====="+config.get("grade"));
            TimeUnit.SECONDS.sleep(5);
        }
    }
}
