package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Register interested a set of event code for a filter bolt.
 * 
 * @author Yanjun
 */
public interface EventInteresteable extends Serializable {

	/**
	 * Register interested event
	 * @param eventCode
	 */
	void interestEvent(String eventCode);
	
	/**
	 * Decide whether a event is interested
	 * @param eventCode
	 * @return
	 */
	boolean isInterestedEvent(String eventCode);
}
