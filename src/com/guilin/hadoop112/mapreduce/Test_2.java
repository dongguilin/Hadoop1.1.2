package com.guilin.hadoop112.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class Test_2 extends Configured implements Tool{
	
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
					String anum = lineSplit[0];//主叫
					String bnum = lineSplit[1];//被叫
					String mac = lineSplit[6];
//					context.write(new Text(bnum),new Text(anum));// 输出
				} catch (ArrayIndexOutOfBoundsException ex) {
					context.getCounter(Counter.LINESKIP).increment(1);// 出错时令计数器加1
					return;
				}
			}
		}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
