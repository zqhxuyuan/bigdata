package org.shirdrn.storm.analytics;

import java.util.Collection;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.api.EventHandler;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.common.GenericEventHandlerManager;

import redis.clients.jedis.Jedis;

public class JedisEventHandlerManager extends GenericEventHandlerManager<Collection<Result>, Jedis, JSONObject> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(JedisEventHandlerManager.class);
	
	@Override
	public void start() {
		super.start();
		ConnectionManager<Jedis> connectionManager = JedisConnectionManager.newInstance();
		for(EventHandler<Collection<Result>, Jedis, JSONObject> handler : eventHandlers.values()) {
			handler.setConnectionManager(connectionManager);
			LOG.info("Initialize event handler: " + handler);
		}
	}

}
