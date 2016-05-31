package com.zqh.zookeeper.dist.lock;

import com.zqh.zookeeper.dist.Constant;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * 启动多次该程序, 然后停止占用锁的那个节点, 观察其他节点的日志
 */
public class LockUnFair implements Watcher {

	private ZooKeeper zk;
	private CountDownLatch down = new CountDownLatch(1);

	static Random random = new Random();
	static Integer current;
	
	public LockUnFair() {
	}

	public LockUnFair(String host) throws Exception {
		this.zk = new ZooKeeper(host, 5000, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
					down.countDown();
				}
			}
		});
		down.await();
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeDeleted) {
			// 如果发现，监听的节点，挂掉了，那么就重新，进行监听
			try {
				System.out.println("节点<" + current + ">去抢占了.......");
				createTemp();
				// check();
			} catch (Exception e) {
				e.printStackTrace();

			}
		}
	}

	public void close() throws Exception {
		zk.close();
	}

	/***
	 * 创建锁node，注意是抢占 的
	 * */
	public void createTemp() throws Exception {
		Thread.sleep(random.nextInt(2500));// 加个线程休眠，实现模拟同步功能
		if (zk.exists("/a/b", this) != null) {
			System.out.println("锁被占用,监听进行中......");
		} else {
			String data = zk.create("/a/b", "a".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("节点<" + current + ">创建锁成功，节点路径:  " + data);
		}
	}
	public void createPersist() throws Exception{
		if (zk.exists("/a", false) == null) {
			String data = zk.create("/a", "a".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("创建根节点......!");
		}
	}

	public static void main(String[] args) throws Exception {
		LockUnFair lock = new LockUnFair(Constant.host);
		current = random.nextInt(100); 
		
		lock.createPersist();//创建主节点
		lock.createTemp();
		Thread.sleep(Long.MAX_VALUE);
		lock.close();
	}

}
