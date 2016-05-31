package cn.kane.redisCluster.zookeeper.watchers;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import cn.kane.redisCluster.cache.man.ICacheManageInterface;
import cn.kane.redisCluster.zookeeper.nodes.NodeInfo;

public class ShardProposerWatcher extends LeaderWatcher {

	private ZooKeeper zkClient;
	private ICacheManageInterface cacheMan ;
	private String nodeName;
	private NodeInfo node ;

	public ShardProposerWatcher(ZooKeeper zkClient,ICacheManageInterface cacheMan,NodeInfo node) {
		this.zkClient = zkClient;
		this.cacheMan = cacheMan ;
		this.node = node ;
		String nodePath = node.getNodePath() ;
		int startIndex = nodePath.lastIndexOf("/");
		this.nodeName = nodePath.substring(startIndex);
	}

	@Override
	public boolean addWatcher(String leaderPath) {
		boolean iAmShardLeader = false;
		boolean isShardMasterExist = true;
		boolean isShardMasterValid = true;
		try {
			String shardMaster = null;
			if (null == zkClient.exists(leaderPath, this)) {
				isShardMasterExist = false;
			}
			if (isShardMasterExist) {
				byte[] leaderNodeBytes = zkClient.getData(leaderPath, this,	null);
				if (null == leaderNodeBytes) {
					isShardMasterValid = false;
				} else {
					shardMaster = new String(leaderNodeBytes);
				}
			}
			if (isShardMasterExist && isShardMasterValid) {
				LOG.info("i am shard-follower,leader is " + shardMaster);
				 this.slaveOfMaster(shardMaster);
			} else {
				LOG.debug("there's no leader,i try to be");
				iAmShardLeader = this.tryToBeShardLeader(leaderPath);
			}
		} catch (KeeperException e) {
			LOG.info("checkIAmLeader-keeperException", e);
		} catch (InterruptedException e) {
			LOG.info("checkIAmLeader-InterruptedException", e);
		} finally {
			try {
				String newLeader = new String(zkClient.getData(leaderPath,false, null));
				node.getShard().setShardLeaderNodeName(newLeader);
				LOG.info("[Leader-Change] newLeader = " + newLeader);
			} catch (Exception e) {
				LOG.info("[Leader]finally-error",e);
			}
		}
		return iAmShardLeader ;
	}

	private boolean tryToBeShardLeader(String leaderPath) throws KeeperException, InterruptedException {
		LOG.info("[ToBeShardLeader]-start ");
		// beLeader
		zkClient.create(leaderPath, cacheMan.cacheServerInfo().getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		LOG.info("[ToBeShardLeader]-[NEW] i am leader " + nodeName);
		cacheMan.beMaster();
		return true;
	}

	private void slaveOfMaster(String shardMaster){
		if(!cacheMan.cacheServerInfo().equals(shardMaster)){
			String[] masterArray = shardMaster.split(":") ;
			String masterHost = masterArray[0] ;
			int masterPort = Integer.parseInt(masterArray[1]) ;
			cacheMan.slaveOf(masterHost, masterPort);
		}else{
			cacheMan.beMaster();
		}
	}
	
	@Override
	public void addNextWacther() {
		ShardLeaderWatcher groupLeaderWatcher = new ShardLeaderWatcher(zkClient, node);
		groupLeaderWatcher.addWatcher(node.getGroup().getGroupLeaderPath());
	}
}