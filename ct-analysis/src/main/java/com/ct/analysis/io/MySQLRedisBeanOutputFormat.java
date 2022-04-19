package com.ct.analysis.io;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import com.ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MYSQL数据格式化输出对象
 */
public class MySQLRedisBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue > {

    protected static class MySQLRecordWriter extends RecordWriter<AnalysisKey,AnalysisValue> {

        private Connection connection = null;

        private Jedis jedis = null;

        public MySQLRecordWriter(){
            //获取资源
            connection = JDBCUtil.getConnnection();
            jedis = new Jedis("hadoop102",6379);
            jedis.auth("zhongtao");
        }

        /**
         * 输出数据
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {

            int sumCall = Integer.parseInt(value.getSumCall());
            int sumDuraiton = Integer.parseInt(value.getSumDruation());

            String telephone = jedis.hget("ct_user",key.getTel());
            String date = jedis.hget("ct_date",key.getDate());

            int userId = -1,dataId = -1;
            if (null != telephone) userId = Integer.parseInt(telephone);
            if (null != date) dataId = Integer.parseInt(date);

            PreparedStatement pstat = null;
            try {
                if (dataId != -1 && userId != -1){
                    String insertSQL = "insert into ct_call (telid,dateid,sumcall,sumduration) values ( ?, ?, ?, ? )";
                    pstat = connection.prepareStatement(insertSQL);

                    pstat.setInt(1,userId);
                    pstat.setInt(2,dataId);
                    pstat.setInt(3,sumCall);
                    pstat.setInt(4,sumDuraiton);

                    pstat.executeUpdate();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }finally {
                if (pstat != null) {
                    try {
                        pstat.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }

        /**
         * 释放资源
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (connection != null){
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (jedis != null) jedis.close();
        }
    }

    @Override
    public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }


    private FileOutputCommitter committer = null;
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }
    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null: new Path(name);
    }
}
