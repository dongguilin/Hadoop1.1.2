package com.guilin.hadoop112.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class ReadMySqlDB {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, StudentRecord, LongWritable, Text> {

		@Override
		public void map(LongWritable key, StudentRecord value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			output.collect(new LongWritable(value.id),
					new Text(value.toString()));

		}

	}

	public static class StudentRecord implements Writable, DBWritable {
		public int id;
		public String name;
		public String sex;
		public int age;

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setInt(1, this.id);
			statement.setString(2, this.name);
			statement.setString(3, this.sex);
			statement.setInt(4, this.age);
		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.id = resultSet.getInt(1);
			this.name = resultSet.getString(2);
			this.sex = resultSet.getString(3);
			this.age = resultSet.getInt(4);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.id);
			Text.writeString(out, this.name);
			Text.writeString(out, this.sex);
			out.writeInt(this.age);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = Text.readString(in);
			this.sex = Text.readString(in);
			this.age = in.readInt();
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, URISyntaxException {
		JobConf conf = new JobConf(ReadMySqlDB.class);
		conf.set("mapred.job.tracker", "master:9001");
		conf.set("mapred.jar", "hadoop-test.jar");

//		FileSystem fileSystem = FileSystem.get(new URI(
//				"hdfs://192.168.137.111:9000"), new Configuration(), "hadoop");
//		DistributedCache.addFileToClassPath(new Path(
//				"hdfs://master:9000/lib/mysql-connector-java-5.1.10.jar"),
//				conf, fileSystem);
		// 设置输入类型
		conf.setInputFormat(DBInputFormat.class);
		// 设置输出类型
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		// 设置map和reduce
		conf.setMapperClass(Map.class);
		conf.setReducerClass(IdentityReducer.class);
		// 设置输出目录
		FileOutputFormat.setOutputPath(conf, new Path(
				"hdfs://master:9000/usr/hadoop/rdb_out"));
		// 建立数据库连接
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://master:3306/school", "root", "guilin");
		// 读取student表中的数据
		String[] fields = { "id", "name", "sex", "age" };
		DBInputFormat.setInput(conf, StudentRecord.class, "student", null,
				"id", fields);
		JobClient.runJob(conf);
	}
}
