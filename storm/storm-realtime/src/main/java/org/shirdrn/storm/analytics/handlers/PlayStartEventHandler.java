package org.shirdrn.storm.analytics.handlers;

import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class PlayStartEventHandler extends JedisEventHandler {

	private static final long serialVersionUID = 1L;
	
	public PlayStartEventHandler(String eventCode) {
		super(eventCode);
	}

	@Override
	public void registerIndicators() {
		// register indicators
		registerIndicatorInternal(StatIndicators.USER_DYNAMIC_INFO); 
		registerIndicatorInternal(StatIndicators.PLAY_NU);
		registerIndicatorInternal(StatIndicators.PLAY_AU);
		registerIndicatorInternal(StatIndicators.PLAY_TIMES);
	}
	
}
