package org.shirdrn.storm.analytics.handlers;

import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class OpenEventHandler extends JedisEventHandler {

	private static final long serialVersionUID = 1L;
	
	public OpenEventHandler(String eventCode) {
		super(eventCode);
	}

	@Override
	public void registerIndicators() {
		// register indicators
		registerIndicatorInternal(StatIndicators.USER_DYNAMIC_INFO);
		registerIndicatorInternal(StatIndicators.OPEN_NU);
		registerIndicatorInternal(StatIndicators.OPEN_AU);
		registerIndicatorInternal(StatIndicators.OPEN_TIMES);
	}

}
