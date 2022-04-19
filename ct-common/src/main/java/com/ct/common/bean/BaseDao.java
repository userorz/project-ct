package com.ct.common.bean;

import com.ct.common.api.Column;
import com.ct.common.api.RowKey;
import com.ct.common.api.TableRef;
import com.ct.common.constant.Names;
import com.ct.common.constant.ValueConstant;
import com.ct.common.util.DateUtil;
import com.ctc.wstx.util.DataUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 基础数据访问对象
 */

public abstract class BaseDao {


    private ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<Admin>();

    protected void start() throws IOException {
        getConnection();
        getAdmin();
    }

    protected void end() throws IOException{
        Admin admin = getAdmin();
        if(admin != null){
            admin.close();
            adminHolder.remove();
        }
        Connection conn = getConnection();
        if(conn != null){
            conn.close();
            connHolder.remove();
        }
    }

    /**
     * 创建表，如果表已经存在，那么删除后在创建新的
     * @param name
     * @param families
     */
    protected void createTableXX(String name,String ... families) throws IOException {
        createTable(name,null,null,families);
    }

    /**
     * 创建表
     * @param name 表明
     * @param regionCount 分区数
     * @param families 列族
     * @throws IOException
     */
    protected void createTableXX(String name,String coprocessorClass,Integer regionCount,String ... families) throws IOException {
        Admin admin = getAdmin();

        TableName tableName = TableName.valueOf(name);

        if(admin.tableExists(tableName)){
            //若表存在，删除表
            deleteTable(name);
        }
        //创建表
        createTable(name,coprocessorClass,regionCount,families);
    }

    private void createTable(String name,String coprocessorClass,Integer regionCount,String ... families) throws IOException{
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(tableName);

        if (families == null || families.length == 0){
            families = new String[]{Names.CF_INFO.getValue()};
        }

        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor =
                    ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        if (coprocessorClass != null && !"".equals(coprocessorClass)){//增加协处理器保存被叫用户信息
            tableDescriptorBuilder.setCoprocessor(coprocessorClass);
            System.out.println("协处理器增加成功");
        }

        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //计算分区键
        if (null == regionCount || regionCount <= 1){
            admin.createTable(tableDescriptor);
        }else{
            byte[][] split = genSplitKeys(regionCount);
            admin.createTable(tableDescriptor,split);
        }
    }

    /**
     * 获取查询时startrow, stoprow集合
     * @param tel
     * @param start
     * @param end
     * @return
     */
    protected List<String[]> getStartSrarRowKeys(String tel, String start , String end){
        List<String[]> list = new ArrayList<String[]>();

        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);
        tel = tel.substring(tel.length() - 4);

        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime, "yyyyMM"));

        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()){

            //当前时间
            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");

            int regionNum = genRegionNum(tel, nowTime);

            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String endRow = startRow + "|";

            list.add(new String[]{startRow,endRow });

            startCal.add(Calendar.MONTH,1);
        }
        return list;
    }

    /**
     * 生成分区键
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(int regionCount){
        int splitKeyCount = regionCount - 1;
        byte[][] bs = new byte[splitKeyCount][];
        // (-∞，0|)，（0|，1|），（1|，+∞）

        ArrayList<byte[]> bytes = new ArrayList<byte[]>();
        for(int i = 0 ; i < splitKeyCount ; ++i){
            String splitKey = i + "|";
//            System.out.println(splitKey);
            bytes.add(Bytes.toBytes(splitKey));
        }
        bytes.toArray(bs);
        return bs;

    }

    /**
     * 删除表格
     * @param name
     * @throws IOException
     */
    protected void deleteTable(String name) throws IOException {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

    }

    /**
     * 创建命名空间，如果命名空间已存在。不需要创建，否在创建新的
     * @param namespace
     */
    protected void createNamespaceNX( String namespace ) throws IOException {
        Admin admin = getAdmin();
        try{
            admin.getNamespaceDescriptor(namespace);
        }catch (NamespaceNotFoundException e){
            NamespaceDescriptor namespaceDescriptor =
                    NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 获取连接对象
     * @return
     * @throws IOException
     */
    protected synchronized Connection getConnection() throws IOException {
        Connection conn = connHolder.get();
        if (null == conn){
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

    /**
     *
     * @return
     * @throws IOException
     */
    protected synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (null == admin){
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 自动封装数据，将对象数据保存到hbase
     * @param obj
     * @throws IOException
     */
    protected void putData(Object obj) throws IOException, IllegalAccessException {
        Class<?> clazz = obj.getClass();
        TableRef tableRef = clazz.getAnnotation(TableRef.class);

        String tableName = tableRef.value();

        Field[] fs = clazz.getDeclaredFields();
        String stringRowkey = "";
        for (Field f : fs) {
            RowKey rowKey = f.getAnnotation(RowKey.class);
            if(rowKey != null){
                f.setAccessible(true);
                stringRowkey = (String) f.get(obj);
                break;
            }
        }

        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(stringRowkey));

        for (Field f : fs) {
            Column column = f.getAnnotation(Column.class);
            if(column != null){
                String family = column.family();
                String colName = column.column();
                if(colName != null && "".equals(colName)){
                    colName = f.getName();
                }
                f.setAccessible(true);
                String value =(String) f.get(obj);
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(colName),Bytes.toBytes(value));
            }
        }

        table.put(put);
        table.close();
    }

    protected void putData(String name ,List<Put> put) throws IOException {
        Connection conn = getConnection();
        Table table = conn.getTable(TableName.valueOf(name));
        table.put(put);
        table.close();
    }

    /**
     * 计算分区号
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum(String tel,String date){
        //一个电话在某年某月的通话记录都在一个分区
        int userCodeHash = tel.substring(tel.length() - 4).hashCode();
        int yearMonthHash = date.substring(0,6).hashCode();

        //crc校验采用异或算法
        int crc = Math.abs(userCodeHash ^ yearMonthHash);

        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

}
