package com.ct.producer.bean;


import com.ct.common.bean.DataIn;
import com.ct.common.bean.DataOut;
import com.ct.common.bean.Producer;
import com.ct.common.util.DateUtil;
import com.ct.common.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 本地数据文件生产者
 */
public class LocalFIleProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile boolean flag = true;

    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    public void produce() {

        try{
            //读取通讯录数据
            List<Contact> contacts = in.read(Contact.class);

//            for (Contact contact : contacts) {
//                System.out.println(contact.toString());
//            }

            while (flag){
                //从通讯录随机查找两个电话号码（主叫，被叫
                int cal1Index = new Random().nextInt(contacts.size());
                int cal2Index = new Random().nextInt(contacts.size());
                while(cal1Index == cal2Index){
                    cal2Index = new Random().nextInt(contacts.size());
                }

                Contact call1 = contacts.get(cal1Index);
                Contact call2 = contacts.get(cal2Index);

                //生产随机通话时间

                String startDate = "20210101000000";
                String endDate = "20220101000000";

                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                long calltime = startTime + (long) ((endTime - startTime) * Math.random());

                String callTimeString = DateUtil.format(new Date(calltime),"yyyyMMddHHmmss");

                //生成随机通话时长

                String duration = NumberUtil.format(new Random().nextInt(5000),4);

                //生成通话记录

                Calllog log = new Calllog(call1.getTel(), call2.getTel(), callTimeString, duration);

                //将通话记录刷写到数据文件中

                System.out.println(log);

                out.write(log);

                Thread.sleep(500);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        if(in != null){
            in.close();
        }
        if(out != null){
            out.close();
        }

    }
}
