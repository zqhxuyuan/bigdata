package org.shirdrn.storm.analytics.common;

import org.apache.log4j.Level;

/**
 * Support to set log related options.
 * 
 * @author Yanjun
 */
public interface Loggingable {

	void setPrintRedisCmdLogLevel(Level logLevel);
	
}
