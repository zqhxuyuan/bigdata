package com.zqh.zookeeper.rwlock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 *  读锁watch回调
 * 
 * @author xiaolong.yuanxl
 */
public class MyReadBusinessLogic implements Watcher{

	private ZooKeeper zkp;
	
	private String bussinessNo;

	public MyReadBusinessLogic(String bussinessNo,ZooKeeper zkp) {
		this.bussinessNo = bussinessNo;
		this.zkp = zkp;
	}
	
	@Override
	public void process(WatchedEvent event) {
		//如果前一个对象上的锁释放了,这里回调获取感知
		if (EventType.NodeDeleted.getIntValue() == event.getType().getIntValue()) {
			System.out.println("[callback read thread] max write node has been deleted, path: " + event.getPath());
			doReadBussiness(bussinessNo,event.getPath());
			//close
			try {
				zkp.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	//mock
	private static void doReadBussiness(String bussinessNo,String path){
		System.out.println("path deleted! " + path + " do My read Bussiness! " + bussinessNo);
	}
	
}