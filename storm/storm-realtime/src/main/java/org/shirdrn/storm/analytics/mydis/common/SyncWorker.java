package org.shirdrn.storm.analytics.mydis.common;

public interface SyncWorker<T, C> extends Runnable {

	void process(C connection) throws Exception;
}
