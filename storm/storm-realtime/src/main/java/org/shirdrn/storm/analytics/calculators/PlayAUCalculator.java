package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.commons.constants.StatIndicators;

import redis.clients.jedis.Jedis;

public class PlayAUCalculator extends OpenAUCalculator {

	public PlayAUCalculator() {
		super(StatIndicators.PLAY_AU);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public StatResult calculate(final Jedis connection, JSONObject event) {
		StatResult statResult = super.calculate(connection, event);
		if(statResult != null) {
			statResult.setIndicator(indicator);
		}
		return statResult;
	}
	
}
