package org.shirdrn.storm.analytics.mydis.common;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.DefaultSyncServer;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.utils.ThreadPoolUtils;
import org.shirdrn.storm.spring.utils.SpringFactory;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class SyncServer implements Server {

	private static final Log LOG = LogFactory.getLog(DefaultSyncServer.class);
	private final Object object = new Object();
	private final Configuration conf;
	static final String contextID = "realtime";
	static final String SPTING_CONFIGS = "classpath*:/applicationContext.xml";
	protected final ApplicationContext applicationContext;
	private final String poolName = "SYNC";
	private final ScheduledExecutorService scheduledExecutorService;
	private final int period;
	private final Set<SyncWorker<?, ?>> syncWorkers = Sets.newHashSet();
	private final Map<Class<SyncWorker<?, ?>>, SyncWorker<?, ?>> syncWorkerClasses = Maps.newHashMap();
	
	public SyncServer(Configuration conf) {
		this.conf = conf;
		// Spring context
		applicationContext = SpringFactory.getContextFactory(contextID, SPTING_CONFIGS).getContext(contextID);
		LOG.info("Spring context initialized: " + applicationContext);
		
		// configure sync server
		int nThreads = conf.getInt(Constants.SYNC_SCHEDULER_THREAD_COUNT, 1);
		scheduledExecutorService = ThreadPoolUtils.newScheduledThreadPool(nThreads, poolName);
		period = conf.getInt(Constants.SYNC_SCHEDULER_PERIOD, 10000);
	}
	
	@SuppressWarnings("unchecked")
	public synchronized void registerSyncWorkers(SyncWorker<?, ?> syncWorker) {
		Class<SyncWorker<?, ?>> clazz = (Class<SyncWorker<?, ?>>) syncWorker.getClass();
		if(!syncWorkerClasses.containsKey(clazz)) {
			syncWorkerClasses.put(clazz, syncWorker);
			syncWorkers.add(syncWorker);
			LOG.info("Sync worker registered: " + clazz + " -> " + syncWorker);
		}
	}
	
	@Override
	public void start() throws Exception {
		LOG.info("All sync workers: " + syncWorkers);
		int sleepInterval = 1000;
		for(SyncWorker<?, ?> syncWorker : syncWorkers) {
			scheduledExecutorService.scheduleAtFixedRate(syncWorker, 500, period, TimeUnit.MILLISECONDS);
			Thread.sleep(sleepInterval);
		}
	}
	
	@Override
	public void close() throws Exception {
		// shutdown thread pool
		scheduledExecutorService.shutdown();
		// server exits
		synchronized(object) {
			object.notify();
		}		
	}
	
	@Override
	public void join() throws InterruptedException {
		synchronized(object) {
			object.wait();
		}		
	}

	public Configuration getConf() {
		return conf;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
}
