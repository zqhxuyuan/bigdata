package com.zqh.zookeeper.dist.ha;

import com.zqh.zookeeper.dist.Constant;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 模拟Zookeeper实现单点故障 自动切换
 * @author 秦东亮  http://qindongliang1922.iteye.com/blog/1985787
 */
public class Slave2 implements Watcher {

	private static final String slave = "node-B";
	private static final String DATA = "b";
	
	public static void main(String[] args) throws Exception {
		Slave2 s = new Slave2(Constant.host);
		s.createTemp();
		s.check();
		Thread.sleep(Long.MAX_VALUE);
		s.close();
	}
	
	public ZooKeeper zk;
	private CountDownLatch count = new CountDownLatch(1);

	public Slave2() {
	}

	public Slave2(String hosts) {
		try {
			zk = new ZooKeeper(hosts, 7000, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						count.countDown();
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			count.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void automicSwitch() throws Exception {
		System.out.println("Master故障，Slave自动切换.......,  时间  " +  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
	}

	public void startMaster() {
		System.out.println(slave + "作为Master 启动了........");
	}

	public void createPersist() throws Exception {
		zk.create(Constant.node, "主节点".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("创建主节点成功........");
	}

	public void createTemp() throws Exception {
		String currentNode = zk.create(Constant.subseq, DATA.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(slave + "创建子节点成功..........." + currentNode);
	}

	public void check() throws Exception {
		List<String> list = zk.getChildren(Constant.node, null);
		Collections.sort(list);
		if (list.isEmpty()) {
			System.out.println("此父路径下面没有节点");
		} else {
			String start = list.get(0);
			String data = new String(zk.getData(Constant.subnode + start, false, null));
			System.out.println(Constant.subnode+start + " : " + data);
			if (data.equals(DATA)) {// 等于本身就启动作为Master
				startMaster();
			} else {
				// 非当前节点
				for (int i = 0; i < list.size(); i++) {
					// 获取那个节点存的此客户端的模拟IP
					String temp = new String(zk.getData(Constant.subnode + list.get(i), false, null));

					if (temp.equals(DATA)) {
						// 因为前面作为首位判断，所以这个出现的位置不可能是首位
						// 需要监听小节点里面的最大的一个节点
						String watchPath = list.get(i - 1);
						System.out.println(slave + "监听的是:  " + watchPath);

						zk.exists(Constant.subnode + watchPath, this);// 监听此节点的详细情况
						break;// 结束循环
					}
				}
			}
		}
	}

	public void close() throws Exception {
		zk.close();
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeDeleted) {
			// 如果发现，监听的节点，挂掉了，那么就重新，进行监听
			try {
				System.out.println("注意有节点挂掉，重新调整监听策略........");
				check();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
