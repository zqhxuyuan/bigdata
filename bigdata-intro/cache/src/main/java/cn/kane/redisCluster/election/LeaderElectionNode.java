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
public class LeaderElectionNode {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionNode.class) ;
	
	private static final String LEADER_DATA_NODE = "/leader" ;
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
	public LeaderElectionNode(String groupName,String nodeName,int sessionTimeOut,String conn,Watcher watcher) throws IOException, KeeperException, InterruptedException{
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
		//leader-data-node
		if(null == zkClient.exists(leaderDataNode, watcher)){
			zkClient.create(leaderDataNode,  null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) ;
		}
		this.addSelf2StandbyList();
	}
	
	private void addSelf2StandbyList() throws KeeperException, InterruptedException{
		//register in leader-standby
		leaderSeq = zkClient.create(leaderStandbyPath+"/", nodeName.getBytes(), ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		this.checkLeader();
	}
	
	private void checkLeader() throws KeeperException, InterruptedException{
		byte[] leaderNodeBytes = zkClient.getData(leaderDataNode, leaderWatcher, null) ;
		List<String> leaderStandby = zkClient.getChildren(leaderStandbyPath, leaderStandbyWatcher) ;
		boolean newLeader = false ;
		if(null == leaderNodeBytes){
			newLeader = true ;
		}else{
			String curLeaderName = new String(leaderNodeBytes) ;
			if(leaderStandby.contains(curLeaderName)){
				LOG.info("[Leader]-[STAY] "+ curLeaderName);
			}else{
				newLeader = true ;
			}
		}
		if(newLeader){
			this.voteForNewLeader(leaderStandby);
		}
	}
	
	private void voteForNewLeader(List<String> leaderStandby) throws KeeperException, InterruptedException{
		Object[] leaderStandbyArray = leaderStandby.toArray() ;
		Arrays.sort(leaderStandbyArray);
		String newLeader = leaderStandbyArray[0].toString() ;
		if(leaderSeq.equals(leaderStandbyPath+"/"+newLeader)){
			LOG.info("[Leader]-[NEW] i am leader " + nodeName);
			zkClient.setData(leaderDataNode, nodeName.getBytes(), ANY_VERSION) ;
		}else{
			LOG.info("[Leader]-[NEW] i am follower");
		}
	}
	
	class LeaderNodeWatcher implements Watcher{
		public void process(WatchedEvent event) {
			LOG.info("[Leader-Change]"+event);
		}
	}
	
	class LeaderStandbyWatcher implements Watcher{
		public void process(WatchedEvent event) {
			LOG.info("[Leader-Standby Change]"+event);
			try {
				LeaderElectionNode.this.checkLeader();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
