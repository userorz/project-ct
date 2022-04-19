package com.ct.producer;

import com.ct.producer.bean.LocalFIleProducer;
import com.ct.producer.io.LocalFIleDataOut;
import com.ct.producer.io.LocalFileDataIn;

import java.io.IOException;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {

        if (null == args || args.length < 2){
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar pathIn pathOut");
            System.exit(1);
        }

        //构建生产者对象
        LocalFIleProducer producer = new LocalFIleProducer();

//        producer.setIn(new LocalFileDataIn("E:\\Code\\project-ct-data\\2.资料\\辅助文档\\contact.log"));
//        producer.setOut(new LocalFIleDataOut("E:\\Code\\project-ct-data\\2.资料\\辅助文档\\call.log"));

        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFIleDataOut(args[1]));

        //生产对象

        producer.produce();

        //关闭生产者对象

        producer.close();

    }
}
