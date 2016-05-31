package cn.kane.redisCluster.zookeeper.watchers;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogBaseWatcher implements Watcher {
	
	private static final Logger LOG = LoggerFactory.getLogger(LogBaseWatcher.class);

	@Override
	public void process(WatchedEvent event) {
		LOG.info("[Watcher] event= " + event.getType() + ",path= " + event.getPath());
		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case SyncConnected:
				LOG.debug("[Wacther]SyncConnected:" + event);
				break;
			case Expired:
				LOG.error("[Wacther]session expired:" + event);
				break;
			case Disconnected:
				LOG.error("[Wacther]session disconnected:" + event);
				break;
			default:
				break;
			}
		} else if (event.getType() == Event.EventType.NodeDataChanged) {
			LOG.info("[Watcher]NodeDataChanged:" + event);
		} else if (event.getType() == Event.EventType.NodeChildrenChanged
				|| event.getType() == Event.EventType.NodeDeleted
				|| event.getType() == Event.EventType.NodeCreated) {
			LOG.info("[Watcher]NodeChanged:" + event);
		} else {
			LOG.warn("Unhandled event:" + event);
		}
	}
}