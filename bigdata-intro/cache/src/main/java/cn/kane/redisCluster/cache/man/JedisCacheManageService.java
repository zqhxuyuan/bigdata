package cn.kane.redisCluster.cache.man;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class JedisCacheManageService implements ICacheManageInterface {

	private static final Logger LOG = LoggerFactory.getLogger(JedisCacheManageService.class) ;
	
	private String host ;
	private int port ;
	private int timeOut ;
	private Jedis jedis ;
	
	public JedisCacheManageService(String host,int port,int timeOut){
		this.host = host ;
		this.port = port ;
		this.timeOut = timeOut ;
		this.jedis = new Jedis(host,port,timeOut);
	}
	
	@Override
	public String ping() {
		LOG.debug(String.format("[Jedis]ping %s , timeOut= %s",this.cacheServerInfo(),timeOut));
		String result = jedis.ping() ;
		if("PONG".equals(result)){
			return "OK";
		}else{
			return "FAILED";
		}
	}

	@Override
	public void slaveOf(String masterHost, int port) {
		LOG.debug(String.format("[Jedis] [%s] be slaveOf [%s:%s]",this.cacheServerInfo(),masterHost,port));
		jedis.slaveof(masterHost, port) ;
	}

	@Override
	public void beMaster() {
		LOG.debug(String.format("[Jedis]beMaster %s",this.cacheServerInfo()));
		jedis.slaveofNoOne() ;
	}

	@Override
	public void reConn() {
		LOG.debug(String.format("[CacheMan]reconn %s",this.cacheServerInfo()));
		try{
			jedis.disconnect();
		}catch(Exception e){
			LOG.warn(String.format("[CacheMan]disconnect-error [%s]",this.cacheServerInfo()),e);
		}finally{
			jedis.connect();
		}
	}

	@Override
	public String cacheServerInfo() {
		String serverInfo = host+":"+port ;
		return serverInfo;
	}
	
	@Override
	public String toString(){
		return this.cacheServerInfo();
	}
}
