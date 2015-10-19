package org.shirdrn.storm.analytics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.TimeoutCache;

import redis.clients.jedis.Jedis;

public class RedisTimeoutCache implements TimeoutCache<Jedis, String, String> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(RedisTimeoutCache.class);
	
	@Override
	public void put(Jedis connection, String key, String value, int expredSecs) {
		connection.set(key, value);
		RealtimeUtils.printRedisCmd(LOG, "SET " + key + " " + value);
		
		connection.expire(key, expredSecs);
		RealtimeUtils.printRedisCmd(LOG, "EXPIRE " + key + " " + expredSecs);
	}

	@Override
	public String get(Jedis client, String key) {
		return client.get(key);
	}


	

}
