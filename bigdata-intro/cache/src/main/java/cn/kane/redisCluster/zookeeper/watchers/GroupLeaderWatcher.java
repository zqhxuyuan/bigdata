package cn.kane.redisCluster.zookeeper.watchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;

import cn.kane.redisCluster.zookeeper.nodes.LivingNodeVO;
import cn.kane.redisCluster.zookeeper.nodes.ZkNodeConstant;

public class GroupLeaderWatcher extends LeaderWatcher {

	private static final int ANY_VERSION = -1 ;
	private ZooKeeper zkClient;

	public GroupLeaderWatcher(ZooKeeper zkClient, String nodeName) {
		this.zkClient = zkClient;
	}
	
	@Override
	public boolean addWatcher(String path) {
		try {
			// watch child-change
			List<String> shardNodes = zkClient.getChildren(path,this);
			LivingNodeVO livingNodes = new LivingNodeVO() ;
			List<String> masterNodes = new ArrayList<String>();
			Map<String,List<String>> slaveNodes = new HashMap<String,List<String>>();
			livingNodes.setMasterNodes(masterNodes);
			livingNodes.setSlaveNodes(slaveNodes);
			for(String shard : shardNodes){
				//master
				String masterPath = shard + ZkNodeConstant.LEADER_NODE ;
				byte[] masterBytes = zkClient.getData(masterPath, false, null);
				String masterValue = new String(masterBytes) ;
				//slave
				List<String> slaves = zkClient.getChildren(shard, null);
				//TODO string-format
				masterNodes.add(masterValue) ;
				slaveNodes.put(masterValue,slaves);
			}
			//JSON
			String livingNodesInStr = livingNodes.toString() ;
			zkClient.setData(path, livingNodesInStr.getBytes(),	ANY_VERSION);
		} catch (Exception e) {
			LOG.error("[LivingNodes-change] exception", e);
		}
		return false;
	}

	@Override
	public void addNextWacther() {
		//nothing
	}

}
