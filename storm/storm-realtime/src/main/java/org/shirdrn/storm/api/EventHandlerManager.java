package org.shirdrn.storm.api;

/**
 * Manage events related meta data, such as event codes, the mapping from event code
 * to event handler instance, etc.
 * 
 * @author Yanjun
 * 
 * @param <RESULT> Computed {@link Result}
 * @param <CONNECTION> Connection object
 * @param <EVENT> Event data object
 */
public interface EventHandlerManager<RESULT, CONNECTION, EVENT> 
			extends EventInteresteable, EventMapper<RESULT, CONNECTION, EVENT>, LifecycleAware {

}
