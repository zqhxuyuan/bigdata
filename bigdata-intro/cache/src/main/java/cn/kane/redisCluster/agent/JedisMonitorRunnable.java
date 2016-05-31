package cn.kane.redisCluster.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.kane.redisCluster.jedis.JedisClient;

/**
 * replace with CacheMonitorRunnable
 * @author chenqingxiang
 *
 */
@Deprecated
class JedisMonitorRunnable implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger(JedisMonitorRunnable.class) ;
	//constant
	private final static Long JEDIS_MONITOR_INTERRAL = 3000L;
	private final static int JEDIS_MONITOR_MAX_FAIL_TIMES = 3;
	//redis configuration
	private String host;
	private int port;
	private int timeOut;
	//instance
	private JedisClient jedisClient;
	private ZkRegNode zkRegNode ;

	public JedisMonitorRunnable(String host, int port, int timeOut,ZkRegNode zkRegNode) {
		this.host = host ;
		this.port = port ;
		this.timeOut = timeOut ;
		this.jedisClient = new JedisClient(host, port, timeOut);
		this.zkRegNode = zkRegNode ;
	}

	public void run() {
		long lastRunMillis = 0L;
		int failTimes = 0;
		while (true) {
			long curMillis = System.currentTimeMillis();
			if (curMillis - lastRunMillis > JEDIS_MONITOR_INTERRAL) {
				lastRunMillis = curMillis;
				String pingResult = null;
				try {
					pingResult = jedisClient.ping();
				} catch (Exception e) {
					LOG.error(String.format("Jedis Monitor[%s:%s] error",host,port), e);
					jedisClient = new JedisClient(host,port,timeOut);
				}
				if ("PONG".equals(pingResult)) {
					LOG.info(String.format("Jedis Monitor[%s:%s] ping success",host,port));
					failTimes = 0;
					if(!zkRegNode.isReg()){
						try {
							zkRegNode.reg();
						} catch (Exception e) {
							LOG.error("[Zk-REG] ERROR",e);
						}
					}
				} else {
					failTimes++;
					if (failTimes >= JEDIS_MONITOR_MAX_FAIL_TIMES) {
						LOG.info(String.format("Jedis Monitor[%s:%s] exceed max-reconn times",host,port));
						// zkClient remove
						try {
							zkRegNode.unreg();
						} catch (Exception e) {
							LOG.error("[Zk-UnReg] ERROR",e);
						}
						LOG.error(String.format("Jedis Monitor[%s:%s] exceed max-reconn times,ping failed with %s",host,port,timeOut));
						
					}
					LOG.warn(String.format("Jedis Monitor[%s:%s] ping failed with %s",host,port,timeOut));
				}
			} else {
				// do nothing,recycle
			}
		}
	}

}