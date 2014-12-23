package com.guilin.hadoop112.rpc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {

	public static final int SERVER_PORT = 12345;
	public static final String SERVER_ADDRESS = "localhost";

	public static void main(String[] args) throws IOException {
		Server server = RPC.getServer(new MyBiz(), SERVER_ADDRESS, SERVER_PORT,
				new Configuration());
		server.start();
	}
}
