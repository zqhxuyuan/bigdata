package org.shirdrn.storm.analytics.common;

import org.apache.commons.logging.Log;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.IndicatorCalculator;
import org.shirdrn.storm.api.Result;

/**
 * A generic indicator calculator who is holding some basic information
 * about a {@link IndicatorCalculator} instance.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}.
 * @param <CONNECTION> Connection object.
 * @param <EVENT> Event object data.
 */
public abstract class GenericIndicatorCalculator<RESULT, CONNECTION, EVENT> implements IndicatorCalculator<RESULT, CONNECTION, EVENT>, Loggingable {

	private static final long serialVersionUID = 1L;
	private Level logLevel;
	protected final int indicator;
	
	public GenericIndicatorCalculator(int indicator) {
		super();
		this.indicator = indicator;
	}
	
	@Override
	public int getIndicator() {
		return indicator;
	}

	@Override
	public void setPrintRedisCmdLogLevel(Level logLevel) {
		this.logLevel = logLevel;
	}
	
	protected void logRedisCmd(Log log, String cmd) {
		RealtimeUtils.printRedisCmd(log, logLevel, cmd);
	}

}
