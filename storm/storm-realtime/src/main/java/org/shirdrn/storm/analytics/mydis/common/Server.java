package org.shirdrn.storm.analytics.mydis.common;

public interface Server {

	/** Start this server. */
	void start() throws Exception;

	/** Stop this server. */
	void close() throws Exception;

	/** Wait for this server to exit. */
	void join() throws InterruptedException;
	
}
