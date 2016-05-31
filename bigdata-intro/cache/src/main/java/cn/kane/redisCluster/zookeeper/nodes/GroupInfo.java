package cn.kane.redisCluster.zookeeper.nodes;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class GroupInfo implements Serializable{

	private static final long serialVersionUID = -875689289231227865L;
	
	private String groupPath ;
	private String groupLeaderPath ;
	private String childsRootPath ;
	private String livingDataNode ;
	
	private String leaderNodeName ;
	
	public GroupInfo(String groupPath){
		this.groupPath = "/" + groupPath  ;
		this.groupLeaderPath = this.groupPath + ZkNodeConstant.LEADER_NODE ;
		this.childsRootPath = this.groupPath + ZkNodeConstant.CHILDREN_PATH ;
		this.livingDataNode = this.groupPath + ZkNodeConstant.LIVEINGS_DATAS_NODE ;
	}
	
	public String getGroupPath() {
		return groupPath;
	}
	public String getGroupLeaderPath() {
		return groupLeaderPath;
	}
	public String getChildsRootPath() {
		return childsRootPath;
	}
	public String getLivingDataNode() {
		return livingDataNode;
	}
	
	public String getLeaderNodeName() {
		return leaderNodeName;
	}
	
	public void setLeaderNodeName(String leaderNodeName) {
		this.leaderNodeName = leaderNodeName;
	}

	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE) ;
	}

}
