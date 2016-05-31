package cn.kane.redisCluster.zookeeper.watchers;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import cn.kane.redisCluster.zookeeper.nodes.NodeInfo;

public class ShardLeaderWatcher extends LeaderWatcher {

	private ZooKeeper zkClient;
	private String nodeName;
	private NodeInfo node ;

	public ShardLeaderWatcher(ZooKeeper zkClient,NodeInfo node) {
		this.zkClient = zkClient;
		this.node = node ;
		String nodePath = node.getNodePath() ;
		int startIndex = nodePath.lastIndexOf("/");
		this.nodeName = nodePath.substring(startIndex);
	}

	@Override
	public boolean addWatcher(String leaderPath) {
		boolean iAmGroupLeader = false ;
		try {
			boolean isShardMasterExist = true;
			boolean isShardMasterValid = true;
			String groupMaster = null;
			if (null == zkClient.exists(leaderPath, this)) {
				isShardMasterExist = false;
			}
			if (isShardMasterExist) {
				byte[] leaderNodeBytes = zkClient.getData(leaderPath, this,	null);
				if (null == leaderNodeBytes) {
					isShardMasterValid = false;
				} else {
					groupMaster = new String(leaderNodeBytes);
				}
			}
			if (isShardMasterExist && isShardMasterValid) {
				LOG.info("i am group-follower,leader is" + groupMaster);
			} else {
				LOG.debug("there's no leader,i try to be");
				iAmGroupLeader = this.tryToBeGroupLeader(leaderPath);
			}
		} catch (KeeperException e) {
			LOG.info("checkIAmLeader-keeperException", e);
		} catch (InterruptedException e) {
			LOG.info("checkIAmLeader-InterruptedException", e);
		} finally {
			try {
				String newLeader = new String(zkClient.getData(leaderPath,false, null));
				node.getGroup().setLeaderNodeName(newLeader);
				LOG.info("[Leader-Change] newLeader = " + newLeader);
			} catch (Exception e) {
				LOG.info("[Leader]finally-error",e);
			}
		}
		return iAmGroupLeader ;
	}

	private boolean tryToBeGroupLeader(String leaderPath)throws KeeperException, InterruptedException {
		LOG.info("[ToBeLeader]-start " + nodeName);
		zkClient.create(leaderPath, nodeName.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		LOG.info("[ToBeLeader]-[NEW] i am leader " + nodeName);
		return true;
	}
	
	@Override
	public void addNextWacther() {
		GroupLeaderWatcher livingNodesWatcher = new GroupLeaderWatcher(zkClient,nodeName ) ;
		livingNodesWatcher.addWatcher(node.getGroup().getLivingDataNode()) ;
	}

}