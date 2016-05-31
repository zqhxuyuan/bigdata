package cn.kane.redisCluster.agent;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.kane.redisCluster.hash.ConsistentHash;
import cn.kane.redisCluster.jedis.JedisClient;

/**
 * replace with NodeInfo
 * @author chenqingxiang
 *
 */
@Deprecated
public class ZkRegNode {

	private static final Logger LOG = LoggerFactory.getLogger(ZkRegNode.class);
	/* 一致性hash虚拟节点数  */
	private static final int CONSISTENTHASH_REPLICAS_NUM = 3 ;
	//path-constant
	public static final String LEADER_DATA_NODE = "/leader";
	public static final String LIVINGS_DATA_NODE = "/living_nodes";
	private static final String LEADER_STANDBY_PATH = "/leader_standby";
	private static final int ANY_VERSION = -1;
	// zk-conn
	private String zkConn;
	private int zkSessionTimeOut;
	private ZooKeeper zkClient = null;
	// watcher
	private Watcher watcher;
	private Watcher leaderWatcher;
	private Watcher leaderStandbyWatcher;
	private Watcher shardLeaderWatcher;
	//redis
	private String redisHost;
	private int redisPort;
	private int redisTimeOut;
	//instance
	private JedisClient jedisClient;
	// node-data
	private String groupName;
	private String shardName;
	private String nodeName;
	private String groupPath ;
	private String groupLeaderDataNode;
	private String groupNodesDataNode;
	private String shardPath ;
	private String shardLeaderDataNode;
	private String livingsDataNode;
	private String leaderSeq;

	private boolean isReg = false ;
	
	public ZkRegNode(String groupName,String shardName,String nodeName, int sessionTimeOut,	String zkConn, Watcher watcher, 
			String redisHost, int redisPort,int redisTimeOut) {
		// zookeeper
		this.zkConn = zkConn;
		this.zkSessionTimeOut = sessionTimeOut;
		this.watcher = watcher;
		//redis
		this.redisHost = redisHost ;
		this.redisPort = redisPort ;
		this.redisTimeOut = redisTimeOut ;
		// node
		this.groupName = groupName;
		this.shardName = shardName ;
		this.nodeName = nodeName;
		//group
		groupPath = groupName ;
		groupLeaderDataNode = groupName + LEADER_DATA_NODE;
		groupNodesDataNode = groupName + LEADER_STANDBY_PATH;
		livingsDataNode = groupName + LIVINGS_DATA_NODE;
		//shard
		shardPath = groupNodesDataNode  + shardName ;
		shardLeaderDataNode = shardPath + LEADER_DATA_NODE ;
	}

	public void reg() throws IOException, KeeperException, InterruptedException{
		LOG.info(String.format("[REG] zk[%s]-Redis[%s:%s]-Data[%s/%s]",zkConn,redisHost,redisPort,groupName,nodeName));
		this.leaderWatcher = new LeaderNodeWatcher();
		this.leaderStandbyWatcher = new LeaderStandbyWatcher();
		this.shardLeaderWatcher = new ShardLeaderNodeWatcher();
		zkClient = new ZooKeeper(zkConn, zkSessionTimeOut, watcher);
		jedisClient = new JedisClient(redisHost, redisPort, redisTimeOut) ;
		// group-root path
		if (null == zkClient.exists(groupPath, watcher)) {
			zkClient.create(groupPath, groupName.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		// group-nodes path
		if (null == zkClient.exists(groupNodesDataNode, watcher)) {
			zkClient.create(groupNodesDataNode, new byte[4],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		// living_nodes path
		if (null == zkClient.exists(livingsDataNode, watcher)) {
			zkClient.create(livingsDataNode, new byte[4],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		// shard-root path
		if (null == zkClient.exists(shardPath, watcher)) {
			zkClient.create(shardPath, shardName.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		// standby
		this.addSelf2StandbyList();
		this.setReg(true);
		LOG.info(String.format("[REG] done zk[%s]-Redis[%s:%s]-Data[%s/%s]",zkConn,redisHost,redisPort,groupName,nodeName));
	}
	
	public void unreg() throws InterruptedException{
		LOG.info(String.format("[UNREG] zk[%s]-Redis[%s:%s]-Data[%s/%s]",zkConn,redisHost,redisPort,groupName,nodeName));
		zkClient.close();
		this.setReg(false);
		LOG.info(String.format("[UNREG] done zk[%s]-Redis[%s:%s]-Data[%s/%s]",zkConn,redisHost,redisPort,groupName,nodeName));
	}
	
	private void addSelf2StandbyList() throws KeeperException,InterruptedException {
		// register in leader-standby
		leaderSeq = zkClient.create(shardPath + "/",
				nodeName.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
		LOG.info("[Standby]standby-node-path:"+leaderSeq);
		this.checkIamShardLeader() ;
	}

	private void checkIamShardLeader() throws KeeperException, InterruptedException{
		boolean amILeader = false ;
		boolean isShardMasterExist = true;
		boolean isShardMasterValid = true;
		String shardMaster = null ;
		if (null == zkClient.exists(shardLeaderDataNode, watcher)) {
			isShardMasterExist = false;
		}
		if (isShardMasterExist) {
			byte[] leaderNodeBytes = zkClient.getData(shardLeaderDataNode,shardLeaderWatcher, null);
			if (null == leaderNodeBytes) {
				isShardMasterValid = false;
			}else{
				shardMaster = new String(leaderNodeBytes) ;
			}
		}
		if (isShardMasterExist && isShardMasterValid) {
			LOG.info("i am shard-follower");
			this.slaveOfMaster(shardMaster);
		} else {
			amILeader = this.tryToBeShardLeader();
		}
		if(amILeader){
			jedisClient.beMaster();
			LOG.info("[GroupLeader]i am shardLeader,and try to be groupLeader");
			this.checkIamGroupLeader();
		}
	}
	
	private void slaveOfMaster(String shardMaster){
		String redisApp = redisHost + ":" + redisPort ;
		if(!redisApp.equals(shardMaster)){
			String[] masterArray = shardMaster.split(":") ;
			String masterHost = masterArray[0] ;
			int masterPort = Integer.parseInt(masterArray[1]) ;
			jedisClient.slaveOf(masterHost, masterPort);
		}else{
			jedisClient.beMaster();
		}
	}
	
	private boolean tryToBeShardLeader() {
		LOG.info("[ToBeShardLeader]-start " + nodeName);
		boolean amILeader = false ;
		// beLeader
		try {
			String redisApp = redisHost + ":" + redisPort ;
			zkClient.create(shardLeaderDataNode, redisApp.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			LOG.info("[ToBeShardLeader]-[NEW] i am leader " + nodeName);
			amILeader = true ;
		} catch (KeeperException e) {
			LOG.info("[ToBeShardLeader]-[NEW] i am follower");
			try {
				byte[] leaderNodeBytes = zkClient.getData(shardLeaderDataNode,shardLeaderWatcher, null);
				LOG.info("[NewShardLeader]" + new String(leaderNodeBytes));
			} catch (Exception e1) {
				LOG.error("ToShardBeLeader new-leader", e1);
			}
		} catch (InterruptedException e) {
			LOG.error("[ToBeShardLeader] interrupt", e);
		}
		return amILeader ;
	}
	private void checkIamGroupLeader() throws KeeperException, InterruptedException {
		boolean isLeaderNodeExist = true;
		boolean isLeaderNodeValid = true;
		if (null == zkClient.exists(groupLeaderDataNode, watcher)) {
			isLeaderNodeExist = false;
		}
		if (isLeaderNodeExist) {
			byte[] leaderNodeBytes = zkClient.getData(groupLeaderDataNode,leaderWatcher, null);
			if (null == leaderNodeBytes) {
				isLeaderNodeValid = false;
			}
		}
		if (isLeaderNodeExist && isLeaderNodeValid) {
			LOG.info("i am follower");
		} else {
			this.tryToBeGroupLeader();
		}
	}

	private void tryToBeGroupLeader() {
		LOG.info("[ToBeLeader]-start " + nodeName);
		// beLeader
		try {
			zkClient.create(groupLeaderDataNode, nodeName.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			// watch child-change
			List<String> livingNodes = zkClient.getChildren(groupNodesDataNode,leaderStandbyWatcher);
			String livingNodesInStr = this.getLivingNodesInString(livingNodes);
			zkClient.setData(livingsDataNode, livingNodesInStr.getBytes(),ANY_VERSION);
			LOG.info("[ToBeLeader]-[NEW] i am leader " + nodeName);
		} catch (KeeperException e) {
			LOG.info("[ToBeLeader]-[NEW] i am follower");
			try {
				byte[] leaderNodeBytes = zkClient.getData(groupLeaderDataNode,leaderWatcher, null);
				LOG.info("[NewLeader]" + new String(leaderNodeBytes));
			} catch (Exception e1) {
				LOG.error("ToBeLeader new-leader", e1);
			}
		} catch (InterruptedException e) {
			LOG.error("[ToBeLeader] interrupt", e);
		}
	}

	private String getLivingNodesInString(List<String> livingNodes) {
		StringBuffer livingBuffer = new StringBuffer();
		if (null != livingNodes) {
			for (String node : livingNodes) {
				livingBuffer.append(node).append(",");
			}
		}
		return livingBuffer.toString();
	}

	public boolean isReg() {
		return isReg;
	}

	private void setReg(boolean isReg) {
		this.isReg = isReg;
	}

	class ShardLeaderNodeWatcher implements Watcher {
		public void process(WatchedEvent event) {
			try {
				ZkRegNode.this.checkIamShardLeader();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					String newLeader = new String(zkClient.getData(event.getPath(), false, null));
					LOG.info("[Leader-Change] newLeader = " + newLeader);
					
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	class LeaderNodeWatcher implements Watcher {
		public void process(WatchedEvent event) {
			try {
				ZkRegNode.this.checkIamGroupLeader();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					String newLeader = new String(zkClient.getData(event.getPath(), false, null));
					LOG.info("[Leader-Change] newLeader = " + newLeader);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	class LeaderStandbyWatcher implements Watcher {
		public void process(WatchedEvent event) {
			try {
				List<String> livingNodes = zkClient.getChildren(groupNodesDataNode,leaderStandbyWatcher);
				ConsistentHash<String> consistentHash = new ConsistentHash<String>(ZkRegNode.CONSISTENTHASH_REPLICAS_NUM, livingNodes); 
				String nodeHashDesc = consistentHash.getNodeHashDesc();
				zkClient.setData(livingsDataNode, nodeHashDesc.getBytes(),ANY_VERSION);
				LOG.info("[Leader-Standby Change] livingNodes = "+ nodeHashDesc);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}