package cn.kane.redisCluster.zookeeper.nodes;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ShardInfo implements Serializable{

	private static final long serialVersionUID = -707440552951524978L;
	private GroupInfo group ;
	private String shardPath ;
	private String shardLeaderPath ;
	private String shardLeaderNodeName ;
	private List<Integer> hashValuss ;
	
	public ShardInfo(String shardName,GroupInfo group){
		this.group = group ;
		this.shardPath = group.getGroupPath() + "/" + shardName ;
		this.shardLeaderPath = this.shardPath + ZkNodeConstant.LEADER_NODE ;
	}
	
	public String getShardPath() {
		return shardPath;
	}
	public void setShardPath(String shardPath) {
		this.shardPath = shardPath;
	}
	public List<Integer> getHashValuss() {
		return hashValuss;
	}
	public void setHashValuss(List<Integer> hashValuss) {
		this.hashValuss = hashValuss;
	}
	public String getShardLeaderPath() {
		return shardLeaderPath;
	}
	public GroupInfo getGroup() {
		return group;
	}
	
	public String getShardLeaderNodeName() {
		return shardLeaderNodeName;
	}
	
	public void setShardLeaderNodeName(String shardLeaderNodeName) {
		this.shardLeaderNodeName = shardLeaderNodeName;
	}
	
	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE) ;
	}

}
