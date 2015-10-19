package org.shirdrn.storm.analytics.mydis.common;

import java.util.Date;
import java.util.LinkedList;

import org.apache.commons.configuration.Configuration;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class AbstractSyncWorker<T extends SyncServer, C> implements SyncWorker<T, C> {

	protected final Configuration conf;
	protected final ApplicationContext context;
	protected final JdbcTemplate jdbcTemplate;
	protected final T syncServer;
	protected final int latestHours;
	
	public AbstractSyncWorker(T syncServer) {
		super();
		this.syncServer = syncServer;
		this.conf = syncServer.getConf();
		context = syncServer.getApplicationContext();
		jdbcTemplate = context.getBean(JdbcTemplate.class);
		latestHours = conf.getInt(Constants.SYNC_LATEST_HOURS, 3);
	}
	
	protected void addCurrentHour(LinkedList<String> hours) {
		String currentHour = DateTimeUtils.format(new Date(), Constants.DT_HOUR_FORMAT);
		if(!hours.contains(currentHour)) {
			hours.addFirst(currentHour);
		}
	}
	
}
