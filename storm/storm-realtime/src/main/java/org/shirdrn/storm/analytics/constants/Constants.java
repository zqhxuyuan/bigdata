package org.shirdrn.storm.analytics.constants;

import org.shirdrn.storm.commons.constants.CommonConstants;

public interface Constants {

	String REALTIME_DISPATCHED_PROCESSOR_PARALLELISM = "realtime.dispatched.processor.parallelism";
	String REALTIME_REDIS_CMD_LOG_LEVEL = "realtime.redis.cmd.log.level";
	
	long DEFAULT_INCREMENT_VALUE = 1L;
	
	String CACHE_ITEM_KEYD_VALUE = "Y";
	int CACHE_ITEM_EXPIRE_TIME = 1 * 60 * 60; // 1 hour
	
	String USER_INFO_KEY = "US"; // user static information
	String USER_BEHAVIOR_KEY = "UD"; // user dynamic information
	
	String DT_EVENT_PATTERN = "yyyy-MM-dd HH:mm:ss";
	String DT_HOUR_PATTERN = "yyyyMMddHH";
	String DT_DATE_PATTERN = "yyyy-MM-dd";
	String USER_INFO_KEY_PREFIX = Constants.USER_INFO_KEY + CommonConstants.REDIS_KEY_NS_SEPARATOR;
	String USER_DYNAMIC_INFO_KEY_PREFIX = Constants.USER_BEHAVIOR_KEY + CommonConstants.REDIS_KEY_NS_SEPARATOR;
}
