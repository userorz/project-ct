package com.ct.comsumer.corprocessor;

import com.ct.common.bean.BaseDao;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.client.Put;
import com.ct.common.constant.Names;
import java.io.IOException;
import java.util.Optional;

/**
 * 使用协处理器保存被叫用户
 *
 *
 * 协处理器
 * 1. 创建类
 * 2. 让表找到协处理类（和表有关联）
 * 3. 将项目打成jar包发布到hbase中（关联的jar包也需要发布），并且需要分发
 */
public class InsertCalleeCoprocessor implements RegionObserver, RegionCoprocessor {
    /**
     * 方法命名规则
     * login
     * logout
     * prePut
     * doPut：模板方法设计模型
     *      存在父子类；
     *      父类搭建算法骨架
     *      1.tel取用户代码 2.取时间年月 3.异或算分区 4.hash散列
     *      do1.tel取后四位 do2.202110 do3. ^ do4. %
     */

    /**
     * 保存主叫用户数据之后，由Hbase自动保存被叫用户数据
     * @param c
     * @param put
     * @param edit
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit) throws IOException {

        Table table = c.getEnvironment().getConnection().getTable(TableName.valueOf(Names.TABLE.getValue()));
        //主叫用户的rowkey
        String rowkey = Bytes.toString(put.getRow());

        //被叫信息
        //genRegionNum(call2,calltime) + "_" +  call1 + "_" + calltime + "_" + call2 + "_" + duration + "_1";
        String[] info = rowkey.split("_");

        String call1 = info[1];
        String call2 = info[3];
        String calltime = info[2];
        String duration = info[4];
        String flag = info[5];

        CoprocessorDao coprocessorDao = new CoprocessorDao();

        if ("1".equals(flag)) {
            String calleeRowKey = coprocessorDao.genRegionNum(call2,calltime) + "_" + call2 + "_" + calltime + "_" + call1 + "_" + duration + "_0";

            Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
            byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
            calleePut.addColumn(calleeFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
            calleePut.addColumn(calleeFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
            calleePut.addColumn(calleeFamily,Bytes.toBytes("calltime"),Bytes.toBytes(calltime));
            calleePut.addColumn(calleeFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));

            table.put(calleePut);

        }

        table.close();
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of((RegionObserver) this);
    }
}

class CoprocessorDao extends BaseDao{
    @Override
    public int genRegionNum(String tel, String date) {
        return super.genRegionNum(tel, date);
    }
}
