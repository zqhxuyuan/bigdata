package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Indicator computation interface.
 * 
 * @author Yanjun
 *
 * @param <RESULT> Computed {@link Result}
 * @param <CONNECTION> Connection object
 * @param <EVENT> Event data object
 */
public interface IndicatorCalculator<RESULT, CONNECTION, EVENT> extends Serializable {

	/**
	 * Compute for the indicator.
	 * @param client
	 * @param event
	 * @return
	 */
	RESULT calculate(final CONNECTION connection, EVENT event);
	
	int getIndicator();
}
