package org.shirdrn.storm.analytics.mydis;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncServer;
import org.shirdrn.storm.analytics.mydis.common.SyncServer;
import org.shirdrn.storm.analytics.mydis.common.SyncWorker;
import org.shirdrn.storm.analytics.mydis.workers.StatSyncWorker;
import org.shirdrn.storm.analytics.mydis.workers.UserSyncWorker;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class DefaultSyncServer extends RedisSyncServer {

	public DefaultSyncServer(Configuration conf) {
		super(conf);
	}
	
	static void register(SyncServer syncServer, SyncWorker<?, ?> syncWorker) {
		syncServer.registerSyncWorkers(syncWorker);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new PropertiesConfiguration("config.properties");
		RedisSyncServer syncServer = new DefaultSyncServer(conf);
		// add SyncWorker
		register(syncServer, new StatSyncWorker(syncServer));
		register(syncServer,
				new UserSyncWorker(syncServer, StatIndicators.PLAY_NU_DURATION, CommonConstants.NS_PLAY_NU_DURATION_USER));
		register(syncServer,
				new UserSyncWorker(syncServer, StatIndicators.PLAY_AU_DURATION, CommonConstants.NS_PLAY_AU_DURATION_USER));
		// start server
		syncServer.start();
		syncServer.join();
	}

}
