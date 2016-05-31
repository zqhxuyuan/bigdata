package cn.kane.redisCluster.zookeeper.watchers;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LeaderWatcher implements Watcher {

	protected static final Logger LOG = LoggerFactory.getLogger(LeaderWatcher.class);
	
	@Override
	public void process(WatchedEvent event) {
		LOG.info("[Watcher] process:{}",event);
		String path = event.getPath() ;
		LOG.debug("[Watcher]add wacther:{}",path);
		boolean isLeader = this.addWatcher(path) ;
		if(isLeader){
			LOG.debug("[Watcher]add next wacther:{}",path);
			this.addNextWacther();
		}
	}

	public abstract boolean addWatcher(String path);
	
	public abstract void addNextWacther();
	
}
