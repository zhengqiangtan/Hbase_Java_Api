package com.hbase.mr;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * mapreduce操作hbase
 * 
 * @author gerry
 *
 */
public class HBaseTableDemo {
	/**
	 * 转换字符串为map对象
	 * 
	 * @param content
	 * @return
	 */
	static Map<String, String> transfoerContent2Map(String content) {
		Map<String, String> map = new HashMap<String, String>();
		int i = 0;
		String key = "";
		StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
		while (tokenizer.hasMoreTokens()) {
			if (++i % 2 == 0) {
				// 当前的值是value
				map.put(key, tokenizer.nextToken());
			} else {
				// 当前的值是key
				key = tokenizer.nextToken();
			}
		}
		return map;
	}

	/**
	 * mapper类，从hbase输入数据
	 * 
	 * @author gerry
	 *
	 */
	static class DemoMapper extends TableMapper<Text, ProductModel> {
		private Text outputKey = new Text();
		private ProductModel outputValue = new ProductModel();

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
			if (content == null) {
				System.err.println("数据格式错误" + content);
				return;
			}
			Map<String, String> map = HBaseTableDemo.transfoerContent2Map(content);
			if (map.containsKey("p_id")) {
				// 产品id存在
				outputKey.set(map.get("p_id"));
			} else {
				System.err.println("数据格式错误" + content);
				return;
			}
			if (map.containsKey("p_name") && map.containsKey("price")) {
				// 数据正常，进行赋值
				outputValue.setId(outputKey.toString());
				outputValue.setName(map.get("p_name"));
				outputValue.setPrice(map.get("price"));
			} else {
				System.err.println("数据格式错误" + content);
				return;
			}
			context.write(outputKey, outputValue);
		}
	}

	/**
	 * mapper类，从hbase输入数据,不使用reducer，直接进行输出到habse
	 * 
	 * @author gerry
	 *
	 */
	static class DemoMapper2 extends TableMapper<ImmutableBytesWritable, Put> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
			if (content == null) {
				System.err.println("数据格式错误" + content);
				return;
			}
			Map<String, String> map = HBaseTableDemo.transfoerContent2Map(content);
			ImmutableBytesWritable outputkey = new ImmutableBytesWritable();
			if (map.containsKey("p_id")) {
				// 产品id存在
				outputkey = new ImmutableBytesWritable(Bytes.toBytes(map.get("p_id")));
			} else {
				System.err.println("数据格式错误" + content);
				return;
			}
			Put put = new Put(Bytes.toBytes(map.get("p_id")));
			if (map.containsKey("p_name") && map.containsKey("price")) {
				// 数据正常，进行赋值
				put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(map.get("p_id")));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(map.get("p_name")));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(map.get("price")));
			} else {
				System.err.println("数据格式错误" + content);
				return;
			}
			context.write(outputkey, put);
		}
	}

	/**
	 * 往hbase输出reducer数
	 * 
	 * @author gerry
	 *
	 */
	static class DemoReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable> {
		@Override
		protected void reduce(Text key, Iterable<ProductModel> values, Context context)
				throws IOException, InterruptedException {
			for (ProductModel value : values) {
				// 我只拿一个，如果有多个产品id的话
				ImmutableBytesWritable outputKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
				Put put = new Put(Bytes.toBytes(key.toString()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
				context.write(outputKey, put);
			}
		}
	}

	/**
	 * 执行入口
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 本地选择: initLocalHbaseMapReducerJobConfig2&initLocalHbaseMapReducerJobConfig
		// 集群选择: initLocalHbaseMapReducerJobConfig2&initFailureLocalHbaseMapReducerJobConfig
		Job job = //initLocalHbaseMapReducerJobConfig3();
				// initLocalHbaseMapReducerJobConfig2();
				// initFailureLocalHbaseMapReducerJobConfig();
				initLocalHbaseMapReducerJobConfig();
		int l = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("执行:" + l);
	}

	/**
	 * 本地正常 运行1
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initLocalHbaseMapReducerJobConfig() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://192.168.100.120"); // hadoop的环境
		conf.set("hbase.zookeeper.quorum", "192.168.100.120"); // hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);

		// 设置mapper相关，mapper从hbase输入
		// 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
		// addDependencyJars集群中，参数必须为true。
		TableMapReduceUtil.initTableMapperJob("data",
				new Scan(),
				DemoMapper.class,
				Text.class,
				ProductModel.class,
				job,
				false);

		// 设置reducer相关，reducer往hbase输出
		// 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
		TableMapReduceUtil.initTableReducerJob("online_product",
				DemoReducer.class,
				job,
				null,
				null,
				null,
				null,
				false);

		return job;
	}

	/**
	 * 本地正常 运行3, 直接使用mapper进行hbase输出，不使用reducer进行输出
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initLocalHbaseMapReducerJobConfig3() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://192.168.100.120"); // hadoop的环境
		conf.set("hbase.zookeeper.quorum", "192.168.100.120"); // hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);

		// 设置mapper相关，mapper从hbase输入
		// 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
		// addDependencyJars集群中，参数必须为true。
		TableMapReduceUtil.initTableMapperJob("data", new Scan(), DemoMapper2.class, ImmutableBytesWritable.class, Put.class, job,
				false);

		// 设置reducer相关，reducer往hbase输出
		// 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
		TableMapReduceUtil.initTableReducerJob("online_product", null, job, null, null, null, null, false);
		job.setNumReduceTasks(0);

		return job;
	}

	/**
	 * 本地运行错误 运行
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initFailureLocalHbaseMapReducerJobConfig() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("fs.defaultFS", "hdfs://192.168.100.120"); // hadoop的环境
		conf.set("hbase.zookeeper.quorum", "192.168.100.120"); // hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);

		// 设置mapper相关，mapper从hbase输入
		TableMapReduceUtil.initTableMapperJob("data", new Scan(), DemoMapper.class, Text.class, ProductModel.class, job);

		// 设置reducer相关，reducer往hbase输出
		TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class, job);

		return job;
	}

	/**
	 * 本地正常 运行2
	 * 
	 * @return
	 * @throws Exception
	 */
	static Job initLocalHbaseMapReducerJobConfig2() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		// 不要hadoop的配置信息，也可以解决initFailureLocalHbaseMapReducerJobConfig的这个问题。
		// conf.set("fs.defaultFS", "hdfs://192.168.100.120"); // hadoop的环境
		conf.set("hbase.zookeeper.quorum", "192.168.100.120"); // hbase zk环境信息

		Job job = Job.getInstance(conf, "demo");
		job.setJarByClass(HBaseTableDemo.class);

		// 设置mapper相关，mapper从hbase输入
		TableMapReduceUtil.initTableMapperJob("data", new Scan(), DemoMapper.class, Text.class, ProductModel.class, job);

		// 设置reducer相关，reducer往hbase输出
		TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class, job);

		return job;
	}
}