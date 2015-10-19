package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.GenericIndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventCode;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.api.CallbackHandler;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.constants.StatIndicators;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;

public class UserDynamicInfoCalculator extends GenericIndicatorCalculator<KeyedResult<JSONObject>, Jedis, JSONObject> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(UserDynamicInfoCalculator.class);

	public UserDynamicInfoCalculator() {
		super(StatIndicators.USER_DYNAMIC_INFO);
	}
	
	@SuppressWarnings("serial")
	@Override
	public KeyedResult<JSONObject> calculate(final Jedis connection, JSONObject event) {
		final String eventCode = event.getString(EventFields.EVENT_CODE);
		String udid = event.getString(EventFields.UDID);
		final String key = Constants.USER_BEHAVIOR_KEY + CommonConstants.REDIS_KEY_NS_SEPARATOR + udid;
		String time = event.getString(EventFields.EVENT_TIME);
		final String strDate = DateTimeUtils.format(time, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
		KeyedResult<JSONObject> keyedObj = new KeyedResult<JSONObject>();
		keyedObj.setKey(key);
		keyedObj.setIndicator(indicator);
		
		// set callback handler for lazy computation
		final KeyedResult<JSONObject> result = keyedObj;
		keyedObj.setCallbackHandler(new CallbackHandler<Jedis>() {

			@Override
			public void callback(final Jedis client) throws Exception {
				JSONObject info = null;
				String field = null;
				if(eventCode.equals(EventCode.OPEN)) {
					// first open date
					field = UserInfoKeys.FIRST_OPEN_DATE; 
					String firstOpenDate = client.hget(key, UserInfoKeys.FIRST_OPEN_DATE);
					if(firstOpenDate == null) {
						info = new JSONObject();
						info.put(UserInfoKeys.FIRST_OPEN_DATE, strDate);
					}
					
					// update LOD
					String latestOpenDate = client.hget(key, UserInfoKeys.LATEST_OPEN_DATE);
					if(latestOpenDate == null || !latestOpenDate.equals(strDate)) {
						updateDate(client, key, UserInfoKeys.LATEST_OPEN_DATE, strDate);
					}
				}
				
				if(eventCode.equals(EventCode.PLAY_START)) {
					// first play date
					field = UserInfoKeys.FIRST_PLAY_DATE; 
					String firstOpenDate = client.hget(key, UserInfoKeys.FIRST_PLAY_DATE);
					if(firstOpenDate == null) {
						info = new JSONObject();
						info.put(UserInfoKeys.FIRST_PLAY_DATE, strDate);
					}
					
					// update LPD
					String latestPlayDate = client.hget(key, UserInfoKeys.LATEST_PLAY_DATE);
					if(latestPlayDate == null || !latestPlayDate.equals(strDate)) {
						updateDate(client, key, UserInfoKeys.LATEST_PLAY_DATE, strDate);
					}
				}
				
				if(info != null) {
					result.setData(info);
					String value = info.getString(field);
					// update FOD/FPD
					updateDate(client, key, field, value);
				}
			}

			private void updateDate(final Jedis client, final String key, String field, final String value) {
				client.hset(key, field, value);
				logRedisCmd(LOG, "HSET " + key + " " + field + " " + value);
			}
			
		});
		
		return keyedObj;
	}
	
}
