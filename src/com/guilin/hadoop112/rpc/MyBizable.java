package com.guilin.hadoop112.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol {

	String hello(String name);

}
