package com.zqh.zookeeper.dist.configsync;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/***
 * Zookeeper实现分布式配置同步
 * 把配置文件保存在Zookeeper的znode里，然后通过Watch来监听数据变化，进而帮我们实现同步
 * @author 秦东亮  http://qindongliang1922.iteye.com/blog/1985661
 */
public class ZKConfig implements Watcher {

	private static String hosts = "localhost:2181";
	private static String path = "/sanxian";

	private ZooKeeper zk;	// Zookeeper实例
	private CountDownLatch countDown = new CountDownLatch(1);// 同步工具
	private static final int TIMIOUT = 5000;// 超时时间
	
	public ZKConfig() throws Exception{
		this(hosts, path);
	}
	
	public ZKConfig(String hosts) throws Exception{
		this(hosts, path);
	}
	
	public ZKConfig(String hosts, String path) throws Exception{
		this.path = path;
		
		zk = new ZooKeeper(hosts, TIMIOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState().SyncConnected == Event.KeeperState.SyncConnected) {
					// 防止在未连接Zookeeper服务器前，执行相关的CURD操作
					countDown.countDown();// 连接初始化，完成，清空计数器
				}
			}
		});
	
		countDown.await();
	}

	public void addOrUpdateData(String data) throws Exception {
		this.addOrUpdateData(path, data);
	}
	
	/***
	 * 写入或更新 数据
	 * @param path 写入路径
	 * @param value 写入的值
	 */
	public void addOrUpdateData(String path, String data) throws Exception {
		Stat stat = zk.exists(path, false);
		if (stat == null) {
			// 没有就创建，并写入
			zk.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("新建，并写入数据成功.. ");
		} else {
			// 存在，就更新
			zk.setData(path, data.getBytes(), -1);
			System.out.println("客户端更新数据成功!");
		}
	}

	/**
	 * 读取数据
	 * @param path 读取的路径
	 * @return 读取数据的内容
	 */
	public String readData() throws Exception {
		return new String(zk.getData(path, this, null));
	}

	/** 关闭zookeeper连接 释放资源 */
	public void close() {
		try {
			zk.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			if (event.getType() == Event.EventType.NodeDataChanged) {
				System.out.println("变化数据:  " + readData());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
