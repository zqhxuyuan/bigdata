package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Map event code to a specified {@link EventHandler} instance.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}
 * @param <CONNECTION> Connection object
 * @param <EVENT> Event data object
 */
public interface EventMapper<RESULT, CONNECTION, EVENT> extends Serializable {

	/**
	 * Establish relationship between event code and {@link EventHandler}.
	 * @param eventCode
	 * @param eventHandler
	 */
	void mapping(String eventCode, EventHandler<RESULT, CONNECTION, EVENT> eventHandler);
	
	/**
	 * Get a {@link EventHandler} according to a given event code.
	 * @param eventCode
	 * @return
	 */
	EventHandler<RESULT, CONNECTION, EVENT> getEventHandler(String eventCode);
}
