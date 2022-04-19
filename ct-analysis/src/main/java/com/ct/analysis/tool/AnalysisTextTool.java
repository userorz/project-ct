package com.ct.analysis.tool;

import com.ct.analysis.io.MySQLTextOutputFormat;
import com.ct.analysis.mapper.AnalysisTextMapper;
import com.ct.analysis.reducer.AnalysisTextReducer;
import com.ct.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class AnalysisTextTool implements Tool{

    @Override
    public int run(String[] args) throws Exception {

        //1、获取job
        Job job = Job.getInstance();

        //2、获取jar包路径
        job.setJarByClass(AnalysisTextTool.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));


        //3、关联mapoer和reducer
        TableMapReduceUtil.initTableMapperJob(
                Names.TABLE.getValue(),
                scan,
                AnalysisTextMapper.class,
                Text.class,
                Text.class,
                job
        );
        job.setReducerClass(AnalysisTextReducer.class);


        //5、设置最终输出的key，value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(MySQLTextOutputFormat.class);

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
