package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.GenericIndicatorCalculator;
import org.shirdrn.storm.analytics.common.StatResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.api.CallbackHandler;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.constants.StatIndicators;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class PlayTimesCalculator extends GenericIndicatorCalculator<StatResult, Jedis, JSONObject> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(PlayTimesCalculator.class);

	public PlayTimesCalculator() {
		super(StatIndicators.PLAY_TIMES);
	}
	
	@SuppressWarnings("serial")
	@Override
	public StatResult calculate(final Jedis connection, JSONObject event) {
		StatResult statResult = null;
		final String udid = event.getString(EventFields.UDID);
		String time = event.getString(EventFields.EVENT_TIME);
		String strHour = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_HOUR_PATTERN);
		// get user device information
		JSONObject user =  RealtimeUtils.getUserInfo(connection, udid);
		if(RealtimeUtils.isInvalidUser(user)) {
			String channel = user.getString(UserInfoKeys.CHANNEL);
			String version = user.getString(UserInfoKeys.VERSION);
			String osType = user.getString(UserInfoKeys.OS_TYPE);
			// create StatResult
			statResult = new StatResult();
			statResult.setOsType(osType);
			statResult.setVersion(version);
			statResult.setChannel(channel);
			statResult.setStrHour(strHour);
			statResult.setIndicator(indicator);
			
			// set callback handler
			final StatResult result = statResult;
			statResult.setCallbackHandler(new CallbackHandler<Jedis>() {

				@Override
				public void callback(final Jedis client) throws Exception {
					String key = result.createKey(CommonConstants.NS_STAT_HKEY);
					String field = result.toField();
					long count = Constants.DEFAULT_INCREMENT_VALUE;
					client.hincrBy(key, field, count);
					logRedisCmd(LOG, "HINCRBY " + key + " " + field + " " + count);
				}
				
			});
		}
		return statResult;
	}
	
}
