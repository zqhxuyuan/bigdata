package com.zqh.zookeeper.rwlock;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
/**
 * 创建线程,产生并发写
 * 
 * @author xiaolong.yuanxl 
 */
public class WriteLockThread implements Runnable {

	private static final int TIMEOUT = 3000;
	
	private static final String WRITE_LOCK_PATH = "/lock/write";
	
	private String name;

	public WriteLockThread(String name) {
		this.name = name;
	}
	
	@Override
	public void run() {
		ZooKeeper zkp = null;
		try {
			// 1.建立zk connect
			zkp = new ZooKeeper("116.211.20.207:2182", TIMEOUT, null);
			
			// 2.创建一个EPHEMERAL类型的节点，会话关闭后它会自动被删除
			String currentPath = zkp.create(WRITE_LOCK_PATH + "/write-", this.name.getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("current: " + currentPath);
			
			// 3.sleep 6秒,故意让其它线程也create这个znode,形成并发的效果,同时与读锁也能够一起产生并发
			Thread.sleep(6000);
			
			// 4.查看当前写锁,并在前一个写锁上注册watcher,形成“九连环”
			List<String> children = zkp.getChildren(WRITE_LOCK_PATH, null);
			Collections.sort(children);//由于前缀一样,因此可以用自然排序
			String current = StringUtils.remove(currentPath, WRITE_LOCK_PATH + "/");
			int index = children.indexOf(current);
			
			// 4.1 如果存在前一个节点,则向前一个节点注册watcher,并在watcher里执行完逻辑后再close
			if (-1 != index && index != 0) {
				System.out.println(this.name + " mine: " + current + " preview: " + children.get(index-1));
				//前一个节点
				String previewNodePath = WRITE_LOCK_PATH + "/" + children.get(index-1);
				Stat stat = zkp.exists(previewNodePath , new MyWriteBusinessLogic(this.name,zkp));
				if (stat != null) {
					System.out.println("[write thread] register watcher in " + previewNodePath + " success!");
				}else {
					//很有可能在46行到50行这段时间内,前一个节点已经释放了锁,所以这里会注册不成功,由于当前节点已经是最小的了,所以可以直接执行逻辑
					System.out.println("preview node has been delete, now I can direct execute write bussiness");
					doWriteBussiness(this.name);
					zkp.close();
				}
			}else {
				//4.2 如果当前节点为第一个,则close
				Thread.sleep(3000);//这里需要等待3秒再关闭连接,形成阻塞,让其他线程注册Watcher成功
				zkp.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			
		}
	}
	
	//mock
	private static synchronized void doWriteBussiness(String bussinessNo){
		System.out.println("do write My Bussiness! " + bussinessNo);
	}
	
}
