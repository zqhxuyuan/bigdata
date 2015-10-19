package com.github.shuliga.context;

import com.github.shuliga.security.Credentials;
import org.apache.commons.io.IOUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: yshuliga
 * Date: 09.01.14 16:58
 */
public class UserContext {

	private static UserContext instance;

	private static Map<String, Credentials> map = new HashMap<String, Credentials>();
	private static String ZK_CONNECTION = "10.184.36.183:2181";
	private static String ZK_USER_CONTEXT_ROOT = "/cep/user_context";
	private ZooKeeper zk;
	private Serializer serializer;

	private UserContext(){
		try {
			zk = new ZooKeeper(ZK_CONNECTION, 1000000, new ZkWatcher());
			serializer = new Serializer();
			initRoot();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void initRoot() throws KeeperException, InterruptedException {
		if (zk.exists("/cep", false) == null){
			zk.create("/cep", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create(ZK_USER_CONTEXT_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	public static UserContext getInstance(){
		if(instance == null){
			synchronized (UserContext.class){
				if (instance == null){
					instance = new UserContext();
				}
			}
		}
		return instance;
	}

	public void put(String token, Credentials credentials){
		try {
			if (zk.exists(getPath(token), false) == null){
				zk.create(getPath(token), serializer.toBytes(credentials), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} else {
				zk.setData(getPath(token), serializer.toBytes(credentials), -1);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String getPath(String token) {
		return ZK_USER_CONTEXT_ROOT + "/" + token;
	}

	public List<Credentials> getAll(){
		List<Credentials> result = new ArrayList<Credentials>();
		try {
			for (String childNode : zk.getChildren(ZK_USER_CONTEXT_ROOT, false)){
				byte[] data = zk.getData(getPath(childNode), false, new Stat());
				result.add(serializer.<Credentials>fromBytes(data));
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return result;
	}

	public Credentials get(String token){
		Credentials result = null;
		try {
				byte[] data = zk.getData(getPath(token), false, new Stat());
				result = serializer.<Credentials>fromBytes(data);
		} catch (KeeperException e) {
		} catch (InterruptedException e) {
		}
		return result;
	}

	public void remove(String token){
		try {
			zk.delete(getPath(token), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public int size(){
		return getAll().size();
	}

	public void close() throws InterruptedException {
		zk.close();
	}

}
