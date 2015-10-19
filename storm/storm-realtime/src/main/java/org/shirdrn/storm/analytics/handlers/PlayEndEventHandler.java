package org.shirdrn.storm.analytics.handlers;

import org.shirdrn.storm.analytics.common.JedisEventHandler;
import org.shirdrn.storm.commons.constants.StatIndicators;

public class PlayEndEventHandler extends JedisEventHandler {

	private static final long serialVersionUID = 1L;
	
	public PlayEndEventHandler(String eventCode) {
		super(eventCode);
	}

	@Override
	public void registerIndicators() {
		// register indicators
		registerIndicatorInternal(StatIndicators.PLAY_NU_DURATION);
		registerIndicatorInternal(StatIndicators.PLAY_AU_DURATION);
	}

}
