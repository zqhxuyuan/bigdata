package org.shirdrn.storm.analytics.handlers;

import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class InstallEventHandler extends JedisEventHandler {

	private static final long serialVersionUID = 1L;
	
	public InstallEventHandler(String eventCode) {
		super(eventCode);
	}

	@Override
	public void registerIndicators() {
		// register indicators
		registerIndicatorInternal(StatIndicators.USER_DEVICE_INFO);		
	}

}
