package org.shirdrn.storm.analytics.mydis.common;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Throwables;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisSyncServer extends SyncServer {

	private static final Log LOG = LogFactory.getLog(RedisSyncServer.class);
	private final JedisPool connectionPool;
	
	public RedisSyncServer(Configuration conf) {
		super(conf);
		// Redis pool
		connectionPool = applicationContext.getBean(JedisPool.class);
		LOG.info("Jedis pool created: " + connectionPool);
	}
	
	public Jedis getConnection() {
		Jedis connection = null;
		try {
			connection = connectionPool.getResource();
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
			throw Throwables.propagate(e);
		}
		return connection;
	}
	
	public void returnConnection(Jedis connection) {
		try {
			connectionPool.returnResource(connection);
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
		}
	}


}
