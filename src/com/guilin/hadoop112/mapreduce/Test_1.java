package com.guilin.hadoop112.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Test_1 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// 运行任务
		int res = ToolRunner.run(new Configuration(), new Test_1(), args);
		System.exit(res);
	}

	// 计数器
	enum Counter {
		LINESKIP// 出错的行
	}

	// map任务
	public static class Map extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			try {
				// 数据处理
				String[] lineSplit = line.split(" ");
				String month = lineSplit[0];
				String time = lineSplit[1];
				String mac = lineSplit[6];
				Text out = new Text(month + ' ' + time + ' ' + mac);
				context.write(NullWritable.get(), out);// 输出 key \t value
			} catch (ArrayIndexOutOfBoundsException ex) {
				context.getCounter(Counter.LINESKIP).increment(1);// 出错时令计数器加1
				return;
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "Test_1.txt");// 任务名
		job.setJarByClass(Test_1.class);// 指定class

		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(Map.class);// 调用上面Map类作为Map任务代码
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);// 指定输出key的格式
		job.setOutputValueClass(Text.class);// 指定输出value的格式

		job.waitForCompletion(true);

		// 输出任务完成情况
		// System.out.println("任务名称："+job.getJobName());
		// System.out.println("任务成功："+(job.isSuccessful()?"是":"否"));
		// System.out.println("输入行数："+job.getCounters().findCounter(key));
		// System.out.println("跳过的行："+job.getCounters().findCounter(Counter.LINESKIP).getValue());

		return job.isSuccessful() ? 0 : 1;
	}

}
