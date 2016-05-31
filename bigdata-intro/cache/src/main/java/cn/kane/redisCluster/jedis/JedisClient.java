package cn.kane.redisCluster.jedis;

import redis.clients.jedis.Jedis;

/**
 * replace with JedisCacheManageService
 * @author chenqingxiang
 *
 */
@Deprecated
public class JedisClient {

	private Jedis jedis ;
	
	public JedisClient(String host,int port,int timeOut){
		this.jedis = new Jedis(host,port,timeOut);
	}

	public String ping(){
		return jedis.ping() ;
	}

	public void slaveOf(String masterHost,int masterPort){
		jedis.slaveof(masterHost, masterPort) ;
	}
	
	public void beMaster(){
		jedis.slaveofNoOne() ;
	}
}
