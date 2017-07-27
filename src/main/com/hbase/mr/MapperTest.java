package com.hbase.mr;
import java.io.IOException;
import java.io.ObjectInput;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * GenericOptionsParser这个类，它的作用是将命令行中参数自动设置到变量conf中。
 * 比如：bin/hadoop jar MyJob.jar com.xxx.MyJobDriver -Dmapred.reduce.tasks=5
 * 这样就可以了，不需要将其硬编码到java代码中，很轻松就可以将参数与代码分离开。
 *
 * bin/hadoop jar MyJob.jar com.xxx.MyJobDriver -Dmapred.reduce.tasks=5 \
   -files ./dict.conf  \
   -libjars lib/commons-beanutils-1.8.3.jar,lib/commons-digester-2.1.jar
 *
 */
public class MapperTest {

    public static void main(String[] args) throws Exception {
        //Configuration conf = new Configuration();
        args = new String[]{"users", "hdfs://hadoop0:9000/tmp/data_hbase"};
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.rootdir", "hdfs://hadoop0:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "hadoop0");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        FileSystem filesys = FileSystem.get(new URI("hdfs://hadoop0:9000"), conf);
        if (filesys.exists(new Path(otherArgs[1]))) {
            filesys.delete(new Path(otherArgs[1]), true);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(MapperTest.class);
        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(otherArgs[0], scan, TestMapper.class, Text.class, Text.class, job);
        //  TableMapReduceUtil.initTableMapperJob("table1", scan, TestMapper.class, Text.class, Text.class, job);
        //TableMapReduceUtil.initTableReducerJob("users", MyReducer.class, job);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit((job.waitForCompletion(true)) ? 0 : 1);

    }

    public static class TestMapper extends TableMapper {
        protected void map(ImmutableBytesWritable key, Result value, org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            String rowkey = new String(value.getRow());
            System.out.println(rowkey);
            StringBuffer sb = new StringBuffer();
            for (KeyValue kv : value.list()) {
                sb.append(new String(kv.getQualifier())).append(",").append(new String(kv.getValue()));
                System.out.println(sb.toString());
            }
            context.write(new Text(rowkey), new Text(sb.toString()));

        }

    }

    public static class MyReducer extends TableReducer {
        long i = 0L;
        protected void reduce(Text key, Iterable values, Context context) throws IOException, InterruptedException {
            Put put = new Put(key.toString().getBytes());
            put.add("user_id".getBytes(), "id".getBytes(), String.valueOf(i++).getBytes());
            StringBuffer sb = new StringBuffer();
            for (Object t : values) {
                sb.append(t.toString()).append("||");
            }
            put.add("user_id".getBytes(), "remark".getBytes(), sb.toString().getBytes());

            context.write(null, put);
        }

    }
}
