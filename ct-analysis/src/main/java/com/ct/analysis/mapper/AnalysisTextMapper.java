package com.ct.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AnalysisTextMapper extends TableMapper<Text, Text> {

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
            context.write(new Text(call1 + "_" + year),new Text(duration));
            //主叫用户  -月
            context.write(new Text(call1 + "_" + month),new Text(duration));
            //主叫用户  -日
            context.write(new Text(call1 + "_" + day),new Text(duration));

            //被叫用户  -年
            context.write(new Text(call2 + "_" + year),new Text(duration));
            //被叫用户  -月
            context.write(new Text(call2 + "_" + month),new Text(duration));
            //被叫用户  -日
            context.write(new Text(call2 + "_" + day),new Text(duration));
    }

}
