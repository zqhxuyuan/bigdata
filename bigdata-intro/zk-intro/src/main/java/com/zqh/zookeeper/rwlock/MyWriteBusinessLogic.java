package com.zqh.zookeeper.rwlock;

import org.apache.zookeeper.WatchedEvent;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 写锁watch回调
 * 
 * @author xiaolong.yuanxl
 */

public class MyWriteBusinessLogic implements Watcher{

	private ZooKeeper zkp;
	
	private String bussinessNo;

	public MyWriteBusinessLogic(String bussinessNo,ZooKeeper zkp) {
		this.bussinessNo = bussinessNo;
		this.zkp = zkp;
	}
	
	@Override
	public void process(WatchedEvent event) {
		//如果前一个对象上的锁释放了,这里回调获取感知
		if (EventType.NodeDeleted.getIntValue() == event.getType().getIntValue()) {
			System.out.println("[callback write thread] preview node has been deleted, path: " + event.getPath());
			doBussiness(bussinessNo,event.getPath());
			try {
				//close
				zkp.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	//mock
	private static void doBussiness(String bussinessNo,String path){
		System.out.println("path deleted! " + path + " do My write Bussiness! " + bussinessNo);
	}
	
}