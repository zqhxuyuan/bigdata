package org.shirdrn.storm.analytics.mydis.common;

import java.util.Date;
import java.util.LinkedList;

import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public abstract class RedisSyncWorker extends AbstractSyncWorker<RedisSyncServer, Jedis> {
	
	public RedisSyncWorker(RedisSyncServer syncServer) {
		super(syncServer);
	}
	
	protected void addCurrentHour(LinkedList<String> hours) {
		String currentHour = DateTimeUtils.format(new Date(), Constants.DT_HOUR_FORMAT);
		if(!hours.contains(currentHour)) {
			hours.addFirst(currentHour);
		}
	}
	
	@Override
	public void run() {
		Jedis connection = syncServer.getConnection();
		try {
			process(connection);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			syncServer.returnConnection(connection);
		}
		
	}

}
