package com.ct.comsumer.dao;

import com.ct.common.bean.BaseDao;
import com.ct.common.constant.Names;
import com.ct.common.constant.ValueConstant;
import com.ct.comsumer.Bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseDao extends BaseDao {

    /**
     *初始化
     */
    public void init() throws IOException {
        start();

        createNamespaceNX(Names.NAMESPACE.getValue());
        createTableXX(Names.TABLE.getValue(), "com.ct.comsumer.corprocessor.InsertCalleeCoprocessor",ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());
        end();
    }

    public void insertData(Calllog log) throws IOException, IllegalAccessException {
        log.setRoeKey(genRegionNum(log.getCall1(),log.getCalltime()) + "_" +  log.getCall1()
                + "_" + log.getCalltime() + "_" + log.getCall2() + "_" + log.getDuration() + "_1");
        putData(log);
    }
    /**
     * 将数据插入hbase中
     * @param value
     * @throws IOException
     */
    public void insertDatas(String value) throws IOException {

        //将通话日志保存到Hbase的表中
        String[] info = value.split("\t");

        String call1 = info[0];
        String call2 = info[1];
        String calltime = info[2];
        String duration = info[3];

        /**
         * rowkey设计 （行
         * 1）长度原则
         *      最大值64KB，推荐长度为10 ~ 100byte
         *      最好是8的倍数。能短则端，rowkey太长会影响性能
         * 2）唯一原则
         *      rowkey应该具备唯一性
         * 3）散列原则
         *      3-1) 盐值散列：不能使用时间戳作为rowkey
         *          在rowkey前增加随机数
         *      3-2）字符串反转
         *              电话号码
         *      3-3）计算分区号：hashMap
         */

        List<Put> puts = new ArrayList<>();


        String rowKey = genRegionNum(call1,calltime) + "_" +  call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";//主叫信息

        Put put = new Put(Bytes.toBytes(rowKey));

        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());

        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        puts.add(put);


//        String calleeRowKey = genRegionNum(call2,calltime) + "_" +  call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";//被叫信息
//
//        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
//
//        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
//
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
//        calleePut.addColumn(calleeFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
//        puts.add(calleePut);
//
        putData(Names.TABLE.getValue(),puts);

    }
}
