package com.ct.analysis.mapper;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AnalysisBeanMapper extends TableMapper<AnalysisKey, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            String rowKey = Bytes.toString(key.get());


            String[] info = rowKey.split("_");

            String call1 = info[1];
            String call2 = info[3];
            String calltime = info[2];
            String duration = info[4];

            String year = calltime.substring(0,4) + "0000";
            String month = calltime.substring(0,6) + "00";
            String day = calltime.substring(0,8);


            //主叫用户  -年
            context.write(new AnalysisKey(call1,year),new Text(duration));
            //主叫用户  -月
            context.write(new AnalysisKey(call1,month),new Text(duration));
            //主叫用户  -日
            context.write(new AnalysisKey(call1,day),new Text(duration));

            //被叫用户  -年
            context.write(new AnalysisKey(call2,year),new Text(duration));
            //被叫用户  -月
            context.write(new AnalysisKey(call2,month),new Text(duration));
            //被叫用户  -日
            context.write(new AnalysisKey(call2,day),new Text(duration));
    }

}
