package com.zqh.zookeeper.rwlock;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 利用zookeeper实现读写锁的demo
 * @author xiaolong.yuanxl 
 */
public class LockDemo {
	
	public static final int corePoolSize = 5;
	public static final int maximumPoolSize = 10;
	public static final int keepAliveTime = 3;
	public static final int maximumLinkedQueueSize = 200000;

	// 固定线程池
	private static final ThreadPoolExecutor pool = new ThreadPoolExecutor(
			corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>(maximumLinkedQueueSize),
			new ThreadPoolExecutor.AbortPolicy());

	public static void main(String[] args) throws IOException {
		//先并发写锁
		for (int i = 0; i < 3; i++) {
			WriteLockThread thread = new WriteLockThread("Thread-Write-" + (i+1));
			pool.execute(thread);
		}
		//后在写锁基础上,并发读锁
		for (int i = 0; i < 3; i++) {
			ReadLockThread thread = new ReadLockThread("Thread-Read-" + (i+1));
			pool.execute(thread);
		}
		
	}
	
	// 应该看到如下的输出
	/*
	 * current: /lock/write/write-0000000038        第一次写
	 * current: /lock/write/write-0000000039        第二次写
	 * current: /lock/write/write-0000000040        第三次写
	 * [read theard] register watcher in /lock/write/write-0000000040 success!   读进程watch最后一个写进程
	 * [read theard] register watcher in /lock/write/write-0000000040 success!
	 * [read theard] register watcher in /lock/write/write-0000000040 success!
	 * Thread-Write-2 mine: write-0000000040 preview: write-0000000039            40,39分别存在前一个节点
	 * Thread-Write-3 mine: write-0000000039 preview: write-0000000038
	 * [write thread] register watcher in /lock/write/write-0000000038 success!   39节点上注册了38
	 * [write thread] register watcher in /lock/write/write-0000000039 success!   40节点上注册了39
	 * [callback write thread] preview node has been deleted, path: /lock/write/write-0000000038
	 * path deleted! /lock/write/write-0000000038 do My write Bussiness! Thread-Write-3
	 * [callback write thread] preview node has been deleted, path: /lock/write/write-0000000039
	 * path deleted! /lock/write/write-0000000039 do My write Bussiness! Thread-Write-2
	 * [callback read thread] max write node has been deleted, path: /lock/write/write-0000000040
	 * [callback read thread] max write node has been deleted, path: /lock/write/write-0000000040
	 * [callback read thread] max write node has been deleted, path: /lock/write/write-0000000040
	 * path deleted! /lock/write/write-0000000040 do My read Bussiness! Thread-Read-3
	 * path deleted! /lock/write/write-0000000040 do My read Bussiness! Thread-Read-2
	 * path deleted! /lock/write/write-0000000040 do My read Bussiness! Thread-Read-1
	 */
	
	
}