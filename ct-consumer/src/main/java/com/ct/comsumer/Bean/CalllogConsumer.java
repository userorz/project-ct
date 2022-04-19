package com.ct.comsumer.Bean;

import com.ct.common.bean.Consumer;
import com.ct.common.constant.Names;
import com.ct.comsumer.dao.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志消费者对象
 */
public class CalllogConsumer implements Consumer {

    /**
     * 消费数据
     */
    public void consume() {

        try {
            //创建配置对象
            Properties prop = new Properties();
            prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));

            //获取flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));
            HBaseDao dao = new HBaseDao();

            //初始化
            dao.init();
            //消费数据
            while( true ){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println(record.value());
//                    dao.insertData(new Calllog(record.value()));
                    dao.insertDatas(record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException {


    }
}
