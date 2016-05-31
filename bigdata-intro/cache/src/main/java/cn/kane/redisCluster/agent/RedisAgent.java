package cn.kane.redisCluster.agent;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * redisAgent: one redis-instance mapping one agent ;
 * replace with NodeFactory
 * @author chenqingxiang
 *
 */
@Deprecated
public class RedisAgent {
	
	private static final Logger LOG = LoggerFactory.getLogger(RedisAgent.class) ;
	
	//data
	private String groupName;
	private String shardName;
	private String nodeName;
	//zookeeper
	private int zkSessionTimeOut;
	private String zkConn;
	private Watcher watcher;
	//redis
	private String redisHost;
	private int redisPort;
	private int redisTimeOut;
	//zookeeper-node
	private ZkRegNode zkRegNode ;
	private Thread jedisMonitorThread ;
	
	public void startup(){
		LOG.info(String.format("[Agent] startup : Redis[%s:%s]-Zk[%s]-Data[%s/%s]",redisHost,redisPort,zkConn,groupName,nodeName));
		//zookeeper-reg
		this.zkRegNode = new ZkRegNode(groupName, shardName,nodeName, zkSessionTimeOut, zkConn, watcher, redisHost, redisPort, redisTimeOut) ;
		//jedis-monitor
		Runnable monitorRunnable = new JedisMonitorRunnable(redisHost, redisPort, redisTimeOut, zkRegNode);
		jedisMonitorThread = new Thread(monitorRunnable,groupName+"-"+nodeName);
		jedisMonitorThread.start();
		LOG.info(String.format("[Agent] startup done : Redis[%s:%s]-Zk[%s]-Data[%s/%s]",redisHost,redisPort,zkConn,groupName,nodeName));
	}
	
	public void shutdown(){
		LOG.info(String.format("[Agent] shutdown : Redis[%s:%s]-Zk[%s]-Data[%s/%s]",redisHost,redisPort,zkConn,groupName,nodeName));
		try {
			this.zkRegNode.unreg();
			jedisMonitorThread.interrupt();
			LOG.info(String.format("[Agent] shutdown done : Redis[%s:%s]-Zk[%s]-Data[%s/%s]",redisHost,redisPort,zkConn,groupName,nodeName));
		} catch (InterruptedException e) {
			LOG.error("[Agent]shutdown error",e);
		}
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public int getZkSessionTimeOut() {
		return zkSessionTimeOut;
	}

	public void setZkSessionTimeOut(int zkSessionTimeOut) {
		this.zkSessionTimeOut = zkSessionTimeOut;
	}

	public String getZkConn() {
		return zkConn;
	}

	public void setZkConn(String zkConn) {
		this.zkConn = zkConn;
	}

	public Watcher getWatcher() {
		return watcher;
	}

	public void setWatcher(Watcher watcher) {
		this.watcher = watcher;
	}

	public String getRedisHost() {
		return redisHost;
	}

	public void setRedisHost(String redisHost) {
		this.redisHost = redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public int getRedisTimeOut() {
		return redisTimeOut;
	}

	public void setRedisTimeOut(int redisTimeOut) {
		this.redisTimeOut = redisTimeOut;
	}

	public String getShardName() {
		return shardName;
	}

	public void setShardName(String shardName) {
		this.shardName = shardName;
	}

}
