package com.ct.comsumer;


import com.ct.common.bean.Consumer;
import com.ct.comsumer.Bean.CalllogConsumer;

import java.io.Closeable;
import java.io.IOException;

/**
 * 启动消费者
 *
 * 使用Kafka消费者获取Flum采集的数据
 * 将数据存储到Hbase中
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException, InterruptedException {

        //创建消费者
        Consumer consumer = new CalllogConsumer();

        consumer.consume();

        consumer.close();
    }
}
