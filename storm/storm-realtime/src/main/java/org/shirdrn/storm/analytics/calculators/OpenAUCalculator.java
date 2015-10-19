package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.RedisTimeoutCache;
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

public class OpenAUCalculator extends GenericIndicatorCalculator<StatResult, Jedis, JSONObject> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(OpenAUCalculator.class);
	private final RedisTimeoutCache timeoutCache = new RedisTimeoutCache();
	
	public OpenAUCalculator() {
		super(StatIndicators.OPEN_AU);
	}
	
	public OpenAUCalculator(int indicator) {
		super(indicator);
	}

	@SuppressWarnings("serial")
	@Override
	public StatResult calculate(final Jedis connection, JSONObject event) {
		StatResult statResult = null;
		final String udid = event.getString(EventFields.UDID);
		String time = event.getString(EventFields.EVENT_TIME);
		final String strHour = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_HOUR_PATTERN);
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
			final StatResult result =  statResult;
			statResult.setCallbackHandler(new CallbackHandler<Jedis>() {

				@Override
				public void callback(final Jedis client) throws Exception {
					String field = result.toField();
					String cacheKey = result.getIndicator() + CommonConstants.REDIS_KEY_NS_SEPARATOR +
							field + CommonConstants.REDIS_KEY_NS_SEPARATOR + udid;
					String status = timeoutCache.get(client, cacheKey);
					if(status == null) {
						// real time update counter in Redis
						String key = result.createKey(CommonConstants.NS_STAT_HKEY);
						long count = Constants.DEFAULT_INCREMENT_VALUE;
						client.hincrBy(key, field, count);
						logRedisCmd(LOG, "HINCRBY " + key + " " + field + " " + count);
						
						// cache user information for this indicator: OPEN_NU <-> 11
						int expire = RealtimeUtils.getExpireTime();
						timeoutCache.put(client, cacheKey, Constants.CACHE_ITEM_KEYD_VALUE, expire);
					}
				}
				
			});
			
		}
		return statResult;
	}

}
