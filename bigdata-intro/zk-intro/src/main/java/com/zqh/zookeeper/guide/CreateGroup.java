package com.zqh.zookeeper.guide;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * CreateGroup A program to create a znode representing a group in ZooKeeper
 * 
 * How to run?
 * 1. install ZooKeeper
 * 2. zkServer start
 * 3. run this app. notice, znode we created named 'zoo' must not exist in znodes
 * @author zqhxuyuan
 */
public class CreateGroup implements Watcher {

	private static final int SESSION_TIMEOUT = 5000;

	private ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);

	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		connectedSignal.await();
	}

	@Override
	public void process(WatchedEvent event) { // Watcher interface
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}

	public void create(String groupName) throws KeeperException, InterruptedException {
		String path = "/" + groupName;
		String createdPath = zk.create(path, null/* data */, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Created " + createdPath);
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public static void main(String[] args) throws Exception {
		CreateGroup createGroup = new CreateGroup();
		// createGroup.connect(args[0]);
		// createGroup.create(args[1]);
		createGroup.connect("localhost");
		createGroup.create("guide");
		createGroup.close();
	}
}
