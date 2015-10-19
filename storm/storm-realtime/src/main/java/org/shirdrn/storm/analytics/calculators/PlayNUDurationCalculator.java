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
import redis.clients.jedis.Transaction;

public class PlayNUDurationCalculator extends GenericIndicatorCalculator<StatResult, Jedis, JSONObject> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(PlayNUDurationCalculator.class);

	public PlayNUDurationCalculator() {
		super(StatIndicators.PLAY_NU_DURATION);
	}
	
	@SuppressWarnings("serial")
	@Override
	public StatResult calculate(final Jedis connection, JSONObject event) {
		StatResult statResult = null;
		final String udid = event.getString(EventFields.UDID);
		String time = event.getString(EventFields.EVENT_TIME);
		final int duration = event.getInt(EventFields.PLAY_DURATION);
		if(duration > 0) {
			String strHour = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_HOUR_PATTERN);
			// get user device information
			JSONObject user =  RealtimeUtils.getUserInfo(connection, udid);
			if(RealtimeUtils.isInvalidUser(user)) {
				// check whether new user play
				boolean isNewUserPlay = RealtimeUtils.isNewUserPlay(connection, udid, user, time);
				if(isNewUserPlay) {
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
							String userKey = result.createKey(CommonConstants.NS_PLAY_NU_DURATION_USER);
							String field = result.toField();
							// save new users for play NU
							// like: <key, field, value>
							// <2311010202::31::NU, 0::A-360::3.1.2::AAAAAAAAAADDDDDDDDD, Y>
							String userField = field + CommonConstants.REDIS_KEY_NS_SEPARATOR + udid;
							String userValue = client.hget(key, userField);
							if(userValue == null) {
								userValue = Constants.CACHE_ITEM_KEYD_VALUE;
								Transaction tx = client.multi();
								tx.hset(userKey, userField, userValue);
								tx.hincrBy(key, field, duration);
								tx.exec();
								logRedisCmd(LOG, "HSET " + userKey + " " + userField + " " + userValue);
								logRedisCmd(LOG, "HINCRBY " + key + " " + field + " " + duration);
							} else {
								client.hincrBy(key, field, duration);
								logRedisCmd(LOG, "HINCRBY " + key + " " + field + " " + duration);
							}
						}
						
					});
				}
			}
		}
		return statResult;
	}

}
