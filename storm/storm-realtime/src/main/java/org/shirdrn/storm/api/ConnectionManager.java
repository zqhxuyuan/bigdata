package org.shirdrn.storm.api;

import java.io.Serializable;

import org.apache.log4j.Level;

/**
 * Manage connection for a used storage engine.
 * 
 * @author Yanjun
 *
 * @param <CONNECTION> Connection object
 */
public interface ConnectionManager<CONNECTION> extends Serializable, LifecycleAware {

	/**
	 * Obtain a available connection object
	 * @return
	 */
	CONNECTION getConnection();
	
	/**
	 * Release connection
	 * @param connection
	 */
	void releaseConnection(CONNECTION connection);
	
	/**
	 * Get storage engine related command log level
	 * @return
	 */
	Level getCmdLogLevel();
}
