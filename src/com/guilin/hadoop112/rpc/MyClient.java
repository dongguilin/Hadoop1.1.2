package com.guilin.hadoop112.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class MyClient {

	public static void main(String[] args) throws IOException {
		MyBizable proxy = (MyBizable) RPC.getProxy(MyBizable.class,
				MyBiz.BIZ_VERSION, new InetSocketAddress(
						MyServer.SERVER_ADDRESS, MyServer.SERVER_PORT),
				new Configuration());
		// 调用接口中的方法
		String result = proxy.hello("world");
		System.out.println(result);
		// 本质是关闭网络连接
		RPC.stopProxy(proxy);
	}
}
