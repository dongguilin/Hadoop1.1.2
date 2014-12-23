package com.guilin.hadoop112.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.Test;

public class HDFSAPI {

	private static final String HDFS_PATH = "hdfs://192.168.137.111:9000";
	private static final String USER = "hadoop";
	private static final String INPUT_DIR = "input";

	@Test
	public void test1() throws MalformedURLException, IOException {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		InputStream in = new URL(HDFS_PATH + "/user/hadoop/input/hello.txt")
				.openStream();
		IOUtils.copyBytes(in, System.out, 1024, true);
	}

	@Test
	public void test2() throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH),
				new Configuration(), USER);

		// 创建文件夹
		fileSystem.mkdirs(new Path(INPUT_DIR));

		Path file = new Path(INPUT_DIR + "/f1000");

		// 上传文件
		FSDataOutputStream out = fileSystem.create(file);
		IOUtils.copyBytes(new FileInputStream("input/hrs.sql"), out, 1024, true);

		// 下载文件，并在控制台打印输出
		FSDataInputStream in = fileSystem.open(file);
		// TODO Eclipse控制台打印，中文乱码
		IOUtils.copyBytes(in, System.out, 1024, true);

		// 删除文件
		fileSystem.delete(file, true);
	}

	// TODO 使用hadoop shell查看hello.txt，有乱码
	@Test
	public void test3() throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH),
				new Configuration(), USER);

		// TODO 下面这种方式不行
		// Configuration con = new Configuration();
		// con.set("fs.default.name", "hdfs://master:9000");
		// FileSystem fileSystem = FileSystem.get(con);

		Path file = new Path(INPUT_DIR + "/hello.txt");
		// 写文件
		FSDataOutputStream outStream = fileSystem.create(file);
		outStream.writeUTF("张三 hello world!");
		outStream.write("李四".getBytes());
		outStream.close();

		// 读文件
		FSDataInputStream inStream = fileSystem.open(file);
		System.out.println(inStream.readUTF());
		byte[] buff = new byte[24];
		inStream.read(buff);
		System.out.println(new String(buff));
		inStream.close();
	}

	@Test
	public void test4() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, USER);
		SnappyCodec c = new SnappyCodec();
		// LzopCodec c = new LzopCodec(); // 常犯的错误是误用LzoCodec。
		c.setConf(conf); // 这一步是必须得
		OutputStream os = c.createOutputStream(fileSystem.create(new Path(
				INPUT_DIR + "/file.txt")));
		os.write("hello 小明".getBytes());
		os.close();
		// 或InputStream is = c.createInputStream(fs.open(new Path(“in.lzo”)));
	}
}
