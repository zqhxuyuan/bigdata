package cn.kane.redisCluster.node;

import java.io.IOException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.zookeeper.ZooKeeper;

import cn.kane.redisCluster.cache.man.CacheManage4TestService;
import cn.kane.redisCluster.cache.man.ICacheManageInterface;
import cn.kane.redisCluster.cache.man.JedisCacheManageService;
import cn.kane.redisCluster.zookeeper.watchers.LogBaseWatcher;

public class NodeConfig {
	//node
	private String groupName ;
	private String shardName ;
	private String nodeName ;
	//zookeeper
	private String zkConnStr ;
	private int zkSessionTimeOut;
	//cache host&port
	private String cacheHost ;
	private int cachePort ;
	private int cacheTimeout ;
	//create-instance
	private ZooKeeper zkClient ;
	private ICacheManageInterface cacheMan ;
	
	public void init() throws IOException{
		zkClient = new ZooKeeper(zkConnStr, zkSessionTimeOut, new LogBaseWatcher());
		cacheMan = new JedisCacheManageService(cacheHost,cachePort,cacheTimeout);
	}
	
	public void init4Test()throws IOException{
		zkClient = new ZooKeeper(zkConnStr, zkSessionTimeOut, new LogBaseWatcher());
		cacheMan = new CacheManage4TestService(cacheHost,cachePort,cacheTimeout);
	}
	
	public String getGroupName() {
		return groupName;
	}
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	public String getShardName() {
		return shardName;
	}
	public void setShardName(String shardName) {
		this.shardName = shardName;
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getZkConnStr() {
		return zkConnStr;
	}
	public void setZkConnStr(String zkConnStr) {
		this.zkConnStr = zkConnStr;
	}
	public int getZkSessionTimeOut() {
		return zkSessionTimeOut;
	}
	public void setZkSessionTimeOut(int zkSessionTimeOut) {
		this.zkSessionTimeOut = zkSessionTimeOut;
	}
	public String getCacheHost() {
		return cacheHost;
	}
	public void setCacheHost(String cacheHost) {
		this.cacheHost = cacheHost;
	}
	public int getCachePort() {
		return cachePort;
	}
	public void setCachePort(int cachePort) {
		this.cachePort = cachePort;
	}
	public int getCacheTimeout() {
		return cacheTimeout;
	}
	public void setCacheTimeout(int cacheTimeout) {
		this.cacheTimeout = cacheTimeout;
	}
	public ZooKeeper getZkClient() {
		return zkClient;
	}
	public ICacheManageInterface getCacheMan() {
		return cacheMan;
	}
	
	@Override
	public String toString(){
		return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}
}
