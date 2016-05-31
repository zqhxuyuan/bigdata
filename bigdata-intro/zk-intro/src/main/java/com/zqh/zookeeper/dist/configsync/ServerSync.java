package com.zqh.zookeeper.dist.configsync;

import java.util.Random;

/**
 * 可以启动多个ServerConfig
 * 然后启动ClientUpdate, 则每个ServerConfig都会收到更新通知
 * @author zqhxuyuan
 */
public class ServerSync {

	public static void main(String[] args) throws Exception {
		// 服务端监听代码
		ZKConfig conf = new ZKConfig();
		System.out.println("模拟服务监听器开始监听........" + (new Random()).nextInt(10));
		
		// ZooKeeper.getData()注册了SyncConfig的Watcher事件. 
		// 当客户端对Path更新数据时, 会调用SyncConfig的process()
		String data = conf.readData();
		
		// 服务器一直监听
		Thread.sleep(Long.MAX_VALUE);
		conf.close();
	}
	
}
