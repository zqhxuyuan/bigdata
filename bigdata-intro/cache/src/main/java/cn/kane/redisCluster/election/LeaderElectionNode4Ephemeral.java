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

@SuppressWarnings("unused")
public class LeaderElectionNode4Ephemeral {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionNode4Ephemeral.class) ;
	
	public static final String LEADER_DATA_NODE = "/leader" ;
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
	private String leaderStandbyPath ;
	private String nodeName ;
	private String leaderSeq ;
	public LeaderElectionNode4Ephemeral(String groupName,String nodeName,int sessionTimeOut,String conn,Watcher watcher) throws IOException, KeeperException, InterruptedException{
		//node
		this.groupName = groupName ;
		leaderDataNode = groupName + LEADER_DATA_NODE ;
		leaderStandbyPath = groupName + LEADER_STANDBY_PATH ;
		this.nodeName = nodeName ;
		this.conn = conn ;
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
			this.voteForNewLeader();
		}
	}
	
	private void voteForNewLeader() throws KeeperException, InterruptedException{
		List<String> leaderStandby = zkClient.getChildren(leaderStandbyPath, null) ;
		Object[] leaderStandbyArray = leaderStandby.toArray() ;
		Arrays.sort(leaderStandbyArray);
		String newLeader = leaderStandbyArray[0].toString() ;
		if(leaderSeq.equals(leaderStandbyPath+"/"+newLeader)){
			LOG.info("[Leader]-[NEW] i am leader " + nodeName);
			//leader-data-node
			if(null == zkClient.exists(leaderDataNode, watcher)){
				//beLeader
				zkClient.create(leaderDataNode,nodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL) ;
				//watch child-change
				zkClient.getChildren(leaderStandbyPath, leaderStandbyWatcher) ;
			}
		}else{
			LOG.info("[Leader]-[NEW] i am follower");
		}
	}
	
	class LeaderNodeWatcher implements Watcher{
		public void process(WatchedEvent event) {
			try {
				LeaderElectionNode4Ephemeral.this.checkLeader();
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
			LOG.info("[Leader-Standby Change]"+event);
		}
	}

}
