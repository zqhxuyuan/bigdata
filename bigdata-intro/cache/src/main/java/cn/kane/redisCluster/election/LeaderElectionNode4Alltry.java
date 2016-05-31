package cn.kane.redisCluster.election;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.kane.redisCluster.cache.man.ICacheManageInterface;
import cn.kane.redisCluster.cache.man.JedisCacheManageService;


@SuppressWarnings("unused")
public class LeaderElectionNode4Alltry {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionNode4Alltry.class) ;
	
	public static final String LEADER_DATA_NODE = "/leader" ;
	public static final String LIVINGS_DATA_NODE = "/living_nodes" ;
	private static final String LEADER_STANDBY_PATH = "/leader_standby" ;
	private static final int ANY_VERSION = -1 ;
	//zk-conn
	private ZooKeeper zkClient = null ;
	private String conn ;
	private int sessionTimeOut ;
	//watcher
	private Watcher watcher ;
	private Watcher leaderWatcher ;
	private Watcher leaderStandbyWatcher ;
	//node-data
	private String groupName ;
	private String leaderDataNode ;
	private String livingsDataNode ;
	private String leaderStandbyPath ;
	private String nodeName ;
	private String leaderSeq ;
	public LeaderElectionNode4Alltry(String groupName,String nodeName,int sessionTimeOut,String zkConn,Watcher watcher,
			String redisHost,int redisPort,int redisTimeOut) throws IOException, KeeperException, InterruptedException{
		//node
		this.groupName = groupName ;
		leaderDataNode = groupName + LEADER_DATA_NODE ;
		leaderStandbyPath = groupName + LEADER_STANDBY_PATH ;
		livingsDataNode = groupName + LIVINGS_DATA_NODE ;
		this.nodeName = nodeName ;
		this.conn = zkConn ;
		this.sessionTimeOut = sessionTimeOut ;
		//watcher
		this.watcher = watcher ;
		this.leaderWatcher = new LeaderNodeWatcher() ;
		this.leaderStandbyWatcher = new LeaderStandbyWatcher() ;
		zkClient = new ZooKeeper(conn, sessionTimeOut, watcher) ;
		//group-root path
		if(null == zkClient.exists(groupName, watcher)){
			zkClient.create(groupName, new byte[4], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
		}
		//living_nodes path
		if(null == zkClient.exists(livingsDataNode, watcher)){
			zkClient.create(livingsDataNode, new byte[4], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
		}
		//monitor-thread
		new Thread(new JedisMonitorRunnable(redisHost, redisPort, redisTimeOut),
				nodeName+"-Monitor").start();
		this.addSelf2StandbyList();
	}
	
	private void addSelf2StandbyList() throws KeeperException, InterruptedException{
		//register in leader-standby
		leaderSeq = zkClient.create(leaderStandbyPath+"/", nodeName.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		this.checkLeader();
	}
	
	private void checkLeader() throws KeeperException, InterruptedException{
		boolean isLeaderNodeExist = true ;
		boolean isLeaderNodeValid = true ;
		
		if(null == zkClient.exists(leaderDataNode, watcher)){
			isLeaderNodeExist = false ;
		}
		if(isLeaderNodeExist){
			byte[] leaderNodeBytes = zkClient.getData(leaderDataNode, leaderWatcher, null) ;
			if(null == leaderNodeBytes){
				isLeaderNodeValid = false ;
			}
		}
		if(isLeaderNodeExist && isLeaderNodeValid){
			LOG.info("i am follower");
		}else{
			this.tryToBeLeader();
		}
	}
	
	private void tryToBeLeader(){
		LOG.info("[ToBeLeader]-start " + nodeName);
		//beLeader
		try {
			zkClient.create(leaderDataNode,nodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) ;
			//watch child-change
			List<String> livingNodes = zkClient.getChildren(leaderStandbyPath, leaderStandbyWatcher) ;
			String livingNodesInStr = this.getLivingNodesInString(livingNodes) ;
			zkClient.setData(livingsDataNode, livingNodesInStr.getBytes(), ANY_VERSION) ;
			LOG.info("[ToBeLeader]-[NEW] i am leader " + nodeName);
		} catch (KeeperException e) {
			LOG.info("[ToBeLeader]-[NEW] i am follower");
			try {
				byte[] leaderNodeBytes = zkClient.getData(leaderDataNode, leaderWatcher, null) ;
				LOG.info("[NewLeader]"+new String(leaderNodeBytes));
			} catch (Exception e1) {
				LOG.error("ToBeLeader new-leader",e1);
			}
		} catch (InterruptedException e) {
			LOG.error("[ToBeLeader] interrupt",e);
		}
	}
	
	private String getLivingNodesInString(List<String> livingNodes){
		StringBuffer livingBuffer = new StringBuffer() ;
		if(null!=livingNodes){
			for(String node : livingNodes){
				livingBuffer.append(node).append(",");
			}
		}
		return livingBuffer.toString() ;
	}
	
	class LeaderNodeWatcher implements Watcher{
		public void process(WatchedEvent event) {
			try {
				LeaderElectionNode4Alltry.this.checkLeader();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}finally{
				try {
					String newLeader = new String(zkClient.getData(event.getPath(), false, null));
					LOG.info("[Leader-Change] newLeader = "+newLeader);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	class LeaderStandbyWatcher implements Watcher{
		public void process(WatchedEvent event) {
			try {
				byte[] livingNodes = zkClient.getData(livingsDataNode,false,null) ;
				LOG.info("[Leader-Standby Change] livingNodes = "+new String(livingNodes));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	class JedisMonitorRunnable implements Runnable{
		private String host ;
		private int port ;
		private int timeOut ;
		private ICacheManageInterface jedisClient ;
		private final Long JEDIS_MONITOR_INTERRAL = 3000L ;
		private final int JEDIS_MONITOR_MAX_FAIL_TIMES = 3 ;
		public JedisMonitorRunnable(String host,int port,int timeOut){
			jedisClient = new JedisCacheManageService(host, port, timeOut);
		}
		public void run() {
			long lastRunMillis = 0L ;
			int failTimes = 0 ;
			while(true){
				long curMillis = System.currentTimeMillis() ;
				if(curMillis - lastRunMillis > JEDIS_MONITOR_INTERRAL){
					lastRunMillis = curMillis ;
					String pingResult = null ;
					try{
						pingResult = jedisClient.ping() ;
					}catch(Exception e){
						LOG.error("Jedis Monitor error",e);
					}
					if("PONG".equals(pingResult)){
						LOG.info("PING SUCCESS");
						failTimes = 0 ;
					}else{
						failTimes++ ;
						if(failTimes>=JEDIS_MONITOR_MAX_FAIL_TIMES){
							//zkClient remove
							LOG.error("Exceed max reconn times,failed");
						}
						LOG.warn("PING FAILED");
					}
				}else{
					//do nothing,recycle
				}
			}
		}
		
	}

}
