package org.shirdrn.storm.analytics.calculators;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.common.GenericIndicatorCalculator;
import org.shirdrn.storm.analytics.common.KeyedResult;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventFields;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.api.CallbackHandler;
import org.shirdrn.storm.commons.constants.StatIndicators;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class UserDeviceInfoCalculator extends GenericIndicatorCalculator<KeyedResult<JSONObject>, Jedis, JSONObject> {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(UserDeviceInfoCalculator.class);
	
	public UserDeviceInfoCalculator() {
		super(StatIndicators.USER_DEVICE_INFO);
	}
	
	@SuppressWarnings("serial")
	@Override
	public KeyedResult<JSONObject> calculate(final Jedis connection, JSONObject event) {
		// install event
		String udid = event.getString(EventFields.UDID);
		
		final String appId = event.getString(EventFields.APP_ID);
		final String channel = event.getString(EventFields.CHANNEL);
		final String version = event.getString(EventFields.VERSION);
		final String osType = event.getString(EventFields.OS_TYPE);
		
		final String userKey = Constants.USER_INFO_KEY_PREFIX + udid;
		
		KeyedResult<JSONObject> keyedObj = new KeyedResult<JSONObject>();
		keyedObj.setIndicator(indicator);
		keyedObj.setKey(userKey);
		
		// set callback handler for lazy computation
		keyedObj.setCallbackHandler(new CallbackHandler<Jedis>() {

			@Override
			public void callback(final Jedis client) throws Exception {
				Transaction tx = client.multi();
				tx.hset(userKey, UserInfoKeys.APP_ID, appId);
				tx.hset(userKey, UserInfoKeys.CHANNEL, channel);
				tx.hset(userKey, UserInfoKeys.VERSION, version);
				tx.hset(userKey, UserInfoKeys.OS_TYPE, osType);
				tx.exec();
				
				logRedisCmd(LOG, "MULTI");
				logRedisCmd(LOG, "HSET " + userKey + " " + UserInfoKeys.APP_ID + " " + appId);
				logRedisCmd(LOG, "HSET " + userKey + " " + UserInfoKeys.CHANNEL + " " + channel);
				logRedisCmd(LOG, "HSET " + userKey + " " + UserInfoKeys.VERSION + " " + version);
				logRedisCmd(LOG, "HSET " + userKey + " " + UserInfoKeys.OS_TYPE + " " + osType);
				logRedisCmd(LOG, "EXEC");
			}
			
		});
		
		return keyedObj;
	}
	
}
