package com.ct.analysis.tool;

import com.ct.analysis.io.MySQLRedisBeanOutputFormat;
import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import com.ct.analysis.mapper.AnalysisBeanMapper;
import com.ct.analysis.reducer.AnalysisBeanReducer;
import com.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class AnalysisBeanTool implements Tool{

    @Override
    public int run(String[] args) throws Exception {

        //1、获取job
        Job job = Job.getInstance();

        //2、获取jar包路径
        job.setJarByClass(AnalysisBeanTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));


        //3、关联mapoer和reducer
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisBeanMapper.class,
                AnalysisKey.class,
                Text.class,
                job
        );
        job.setReducerClass(AnalysisBeanReducer.class);


        //5、设置最终输出的key，value类型
        job.setOutputKeyClass(AnalysisKey.class);
        job.setOutputValueClass(AnalysisValue.class);

        job.setOutputFormatClass(MySQLRedisBeanOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        return result ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();

    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
