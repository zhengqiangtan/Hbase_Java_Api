package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * HBASE MAPREDUCE READ & WRITE
 * Created by TZQ on 2017/7/26 0026.
 */
public class User2OtherMR extends Configured implements Tool{
    //Mapper class
    public static class ReadUserMapper extends TableMapper<Text,Put>  {
        private Text mapOutputKey=new Text();

        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //1.get rowkey
            String rowkey = Bytes.toString(key.get());
            //2. set
            mapOutputKey.set(rowkey);

            //3. put
            Put put=new Put(key.get());
            for (Cell cell:value.rawCells()){
                // add family: info
                if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                    // add column:name
                    if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(cell);
                    }
                    // add column:age
                    if("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(cell);
                    }
                }
            }
            //4. context write
            context.write(mapOutputKey,put);

        }
    }

    ///Reduce class
    public static  class WriteUserReducer extends TableReducer<Text,Put,ImmutableBytesWritable> {
        @Override
        public void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put:values){
                context.write(null,put);
            }
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        //1.create job
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        //2.set run job class
        job.setJarByClass(this.getClass());
        //3. set job
        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        //4. set input set mapper
        TableMapReduceUtil.initTableMapperJob(
                "user",      // input table
                scan,             // Scan instance to control CF and attribute selection
                ReadUserMapper.class,   // mapper class
                Text.class,             // mapper output key
                Put.class,             // mapper output value
                job);
        //5.set output set reducer
        TableMapReduceUtil.initTableReducerJob(
                "other",      // output table
                WriteUserReducer.class, // reducer class
                job);

        job.setNumReduceTasks(0);// at least one, adjust as required

        //6.submit job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        //1.get config
        Configuration config = HBaseConfiguration.create();
        //2.submit job
        int status = ToolRunner.run(config, new User2OtherMR(), args);
        //3. exit program
        System.exit(status);
    }
}
