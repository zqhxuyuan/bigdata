package org.shirdrn.storm.analytics;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.spring.utils.SpringFactory;
import org.springframework.context.ApplicationContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Jedis connection manager, who manages Redis connections.
 * 
 * @author yanjun
 */
public class JedisConnectionManager implements ConnectionManager<Jedis> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(JedisConnectionManager.class);
	private static final String contextID = "realtime";
	private static final String SPTING_CONFIGS = "classpath*:/applicationContext.xml";
	private transient ApplicationContext applicationContext;
	private transient JedisPool connectionPool;
	private Level redisCmdLogLevel = Level.DEBUG;
	private static final ConnectionManager<Jedis> INSTANCE = new JedisConnectionManager();
	private static final AtomicBoolean started = new AtomicBoolean(false);
	
	private JedisConnectionManager() {
		super();
	}
	
	public static ConnectionManager<Jedis> newInstance() {
		if(!started.get()) {
			INSTANCE.start();
		}
		return INSTANCE;
	}
	
	@Override
	public Jedis getConnection() {
		Jedis connection = null;
		try {
			checkPool();
			connection = connectionPool.getResource();
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
			throw Throwables.propagate(e);
		}
		return connection;
	}

	@Override
	public void releaseConnection(Jedis connection) {
		try {
			checkPool();
			connectionPool.returnResource(connection);
		} catch (Exception e) {
			connectionPool.returnBrokenResource(connection);
		}		
	}

	private void checkPool() {
		Preconditions.checkArgument(connectionPool != null, "Maybe never invoke start() mechod.");
	}
	
	@Override
	public Level getCmdLogLevel() {
		return redisCmdLogLevel;
	}

	@Override
	public void start() {
		// Spring context
		applicationContext = SpringFactory.getContextFactory(contextID, SPTING_CONFIGS).getContext(contextID);
		LOG.info("Spring context initialized: " + applicationContext);
		
		connectionPool = applicationContext.getBean(JedisPool.class);
		LOG.info("Jedis pool created: " + connectionPool);
		
		Configuration conf = RealtimeUtils.getDefaultConfiguration();
		
		// set print Redis cmd log level
		String level = conf.getString(Constants.REALTIME_REDIS_CMD_LOG_LEVEL);
		if(level != null) {
			redisCmdLogLevel = RealtimeUtils.parseLevel(level);
		}
		LOG.info("Print redis command log level: " + redisCmdLogLevel.toString());
		
		started.compareAndSet(false, true);
	}

	@Override
	public void stop() {
		connectionPool.destroy();
	}

}
