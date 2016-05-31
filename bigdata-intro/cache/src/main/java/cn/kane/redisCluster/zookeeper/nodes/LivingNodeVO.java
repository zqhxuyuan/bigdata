package cn.kane.redisCluster.zookeeper.nodes;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class LivingNodeVO implements Serializable{

	private static final long serialVersionUID = -5910672761877055148L;
	private List<String> masterNodes ;
	private Map<String,List<String>> slaveNodes ;
	public List<String> getMasterNodes() {
		return masterNodes;
	}
	public void setMasterNodes(List<String> masterNodes) {
		this.masterNodes = masterNodes;
	}
	public Map<String, List<String>> getSlaveNodes() {
		return slaveNodes;
	}
	public void setSlaveNodes(Map<String, List<String>> slaveNodes) {
		this.slaveNodes = slaveNodes;
	}

	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE) ;
	}
	
}
