package com.guilin.hadoop112.rpc;

import java.io.IOException;

public class MyBiz implements MyBizable {

	public static long BIZ_VERSION = 22812L;

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return BIZ_VERSION;
	}

	@Override
	public String hello(String name) {
		System.out.println("我被调用了");
		return "hello " + name;
	}

}
