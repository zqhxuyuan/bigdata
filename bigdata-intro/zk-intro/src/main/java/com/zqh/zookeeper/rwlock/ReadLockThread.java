package com.zqh.zookeeper.rwlock;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 创建线程,产生并发读
 * 
 * @author xiaolong.yuanxl 
 */
public class ReadLockThread implements Runnable {

	private static final int TIMEOUT = 3000;
	
	private static final String WRITE_LOCK_PATH = "/lock/write";

	private static final String WRITE_SIGNAL = "write-";
	
	private String name;

	public ReadLockThread(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		ZooKeeper zkp = null;
		try {
			// 1.建立zk connect
			zkp = new ZooKeeper("116.211.20.207:2182", TIMEOUT, null);
			// 2.创建一个EPHEMERAL类型的节点，会话关闭后它会自动被删除
			zkp.create("/lock/read/read-", this.name.getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			// 3.sleep 1秒,故意让其它线程也create这个znode,形成读并发的效果
			Thread.sleep(1000);
			// 4.查看当前写锁,并在最后一个写锁上注册watcher
			List<String> children = zkp.getChildren(WRITE_LOCK_PATH, null);
			String maxWriteLockName = maxSeq(children);
			// 5.在写锁上注册wathcer
			String writeLock = WRITE_LOCK_PATH + "/" + maxWriteLockName;
			Stat stat = zkp.exists(writeLock , new MyReadBusinessLogic(this.name,zkp));
			if (stat != null) {
				System.out.println("[read theard] register watcher in " + writeLock + " success!");
			}else {
				//很有可能在40行到44行这段时间内,写锁已经被释放了,所以,这里直接执行读逻辑
				System.out.println("lastest writeLock node has been delete, now I can direct execute read bussiness");
				doReadBussiness(this.name);
				zkp.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}

	}

	//获取最大seq
	private static synchronized String maxSeq(List<String> list) {
		String maxSeq = "";
		int max = -1; //初始化
		if (list != null && !list.isEmpty()) {
			if (StringUtils.containsIgnoreCase(list.get(0), WRITE_SIGNAL)) {
				String m = StringUtils.remove(list.get(0), WRITE_SIGNAL);
				max = Integer.parseInt(m);
				maxSeq = list.get(0);
			}
			for (int i = 1; i < list.size(); i++) {
				if (StringUtils.containsIgnoreCase(list.get(i), WRITE_SIGNAL)) {
					int temp = Integer.parseInt(StringUtils.remove(list.get(i), WRITE_SIGNAL));
					if (max < temp){
						max = temp;
						maxSeq = list.get(i);
					}	
				}
			}
			
		}
		return maxSeq;
	}
	
	//mock
	private static synchronized void doReadBussiness(String bussinessNo){
		System.out.println("do read My Bussiness! " + bussinessNo);
	}
	
}
