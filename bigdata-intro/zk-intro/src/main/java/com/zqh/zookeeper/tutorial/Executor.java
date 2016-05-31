package com.zqh.zookeeper.tutorial;

/**
 * A simple example program to use DataMonitor to start and stop executables based on a znode. 
 * The program watches the specified znode and 监视指定的znode
 * saves the data that corresponds to the znode in the filesystem.znode节点也可以保存数据 
 * It also starts the specified program with the specified arguments when the znode exists  
 * and kills the program if the znode goes away.
 */

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * http://zookeeper.apache.org/doc/trunk/javaExample.html
 * 
 * 代码分析: 	https://github.com/llohellohe/zookeeper/blob/master/docs/java-example.md
 * 类图:			https://raw.github.com/llohellohe/zookeeper/master/docs/class-java-example.png
 * 
 * How to run this app?
 * 1. $ zkServer.sh start
 * 2. $ zkCli.sh
 *         > create /test my_data			# if exist, don't create it
 * 3. run this app in eclipse
 * 4.		> set /test new_data
 * 5. 观察eclipse的控制台的信息, 当对znode节点修改, 删除节点时, 会触发Watcher. 下面是控制台打印的一些信息: 
ChangeEvent: 										# run this app
     state: SyncConnected
     type: None
Starting child
new_data3											# exec program, param in main

ChangeEvent: 										# change znode data
     state: SyncConnected
     type: NodeDataChanged
Stopping child

Starting child
new_data4
ChangeEvent:										# delete znode 
     state: SyncConnected
     type: NodeDeleted
Killing process
 */
public class Executor implements Watcher, Runnable, DataMonitor.DataMonitorListener {
	
    String znode;
    DataMonitor dm;
    ZooKeeper zk;
    String filename;
    String exec[];
    Process child;

    public Executor(String hostPort, String znode, String filename,
            String exec[]) throws KeeperException, IOException {
        this.filename = filename;
        this.exec = exec;
        zk = new ZooKeeper(hostPort, 3000, this);
        dm = new DataMonitor(zk, znode, null, this);
    }

    public static void main(String[] args) {
    	args = new String[]{"localhost", "/test", "/home/zqhxuyuan/data/zookeeper/executor", "cat", "/home/zqhxuyuan/data/zookeeper/executor"};
        if (args.length < 4) {
            System.err.println("USAGE: Executor hostPort znode filename program [args ...]");
            System.exit(2);
        }
    	
        String hostPort = args[0];
        String znode = args[1];
        String filename = args[2];
        String exec[] = new String[args.length - 3];
        System.arraycopy(args, 3, exec, 0, exec.length);
        try {
            new Executor(hostPort, znode, filename, exec).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * We do process any events ourselves, we just need to forward them on.
     */
    public void process(WatchedEvent event) {
        dm.process(event);
    }

    public void run() {
        try {
            synchronized (this) {
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
        }
    }

    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    static class StreamWriter extends Thread {
        OutputStream os;

        InputStream is;

        StreamWriter(InputStream is, OutputStream os) {
            this.is = is;
            this.os = os;
            start();
        }

        public void run() {
            byte b[] = new byte[80];
            int rc;
            try {
                while ((rc = is.read(b)) > 0) {
                    os.write(b, 0, rc);
                }
            } catch (IOException e) {
            }

        }
    }

    public void exists(byte[] data) {
        if (data == null) {
            if (child != null) {
                System.out.println("Killing process");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                }
            }
            child = null;
        } else {
            if (child != null) {
                System.out.println("Stopping child");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                FileOutputStream fos = new FileOutputStream(filename);
                fos.write(data);
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                System.out.println("Starting child");
                // 执行exec命令, 将结果写入到filename指定的文件中
                // 上面main中的参数将zk.getData(znode)的结果写入到文件中. 实际上是命令行上set /test data的data
                child = Runtime.getRuntime().exec(exec);
                new StreamWriter(child.getInputStream(), System.out);
                new StreamWriter(child.getErrorStream(), System.err);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
