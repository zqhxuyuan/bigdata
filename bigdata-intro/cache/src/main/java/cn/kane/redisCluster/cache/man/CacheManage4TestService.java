package cn.kane.redisCluster.cache.man;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheManage4TestService implements ICacheManageInterface {

	private static final Logger LOG = LoggerFactory.getLogger(CacheManage4TestService.class) ;

	private String host ;
	private int port ;
	private int timeOut;
	
	public CacheManage4TestService(String host,int port,int timeOut){
		this.host = host ;
		this.port = port ;
		this.timeOut = timeOut ;
	}
	
	@Override
	public String ping() {
		LOG.info(String.format("[CacheMan]ping %s:%s, timeOut=%s",host,port,timeOut));
		return "OK" ;
	}

	@Override
	public void slaveOf(String masterHost, int port) {
		LOG.info(String.format("[CacheMan] [%s:%s] be slaveOf [%s:%s]",this.host,this.port,masterHost,port));
	}

	@Override
	public void beMaster() {
		LOG.info(String.format("[CacheMan]beMaster %s:%s",host,port));
	}

	@Override
	public void reConn() {
		LOG.info(String.format("[CacheMan]reconn %s:%s",host,port));
	}

	@Override
	public String cacheServerInfo() {
		String serverInfo = host+":"+port ;
		return serverInfo;
	}

}
