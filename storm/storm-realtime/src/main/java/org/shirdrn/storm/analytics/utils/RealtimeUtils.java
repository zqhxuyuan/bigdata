package org.shirdrn.storm.analytics.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import joptsimple.internal.Strings;
import net.sf.json.JSONObject;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.shirdrn.storm.analytics.JedisEventHandlerManager;
import org.shirdrn.storm.analytics.calculators.OpenAUCalculator;
import org.shirdrn.storm.analytics.calculators.OpenNUCalculator;
import org.shirdrn.storm.analytics.calculators.OpenTimesCalculator;
import org.shirdrn.storm.analytics.calculators.PlayAUCalculator;
import org.shirdrn.storm.analytics.calculators.PlayAUDurationCalculator;
import org.shirdrn.storm.analytics.calculators.PlayNUCalculator;
import org.shirdrn.storm.analytics.calculators.PlayNUDurationCalculator;
import org.shirdrn.storm.analytics.calculators.PlayTimesCalculator;
import org.shirdrn.storm.analytics.calculators.UserDeviceInfoCalculator;
import org.shirdrn.storm.analytics.calculators.UserDynamicInfoCalculator;
import org.shirdrn.storm.analytics.constants.Constants;
import org.shirdrn.storm.analytics.constants.EventCode;
import org.shirdrn.storm.analytics.constants.UserInfoKeys;
import org.shirdrn.storm.analytics.handlers.InstallEventHandler;
import org.shirdrn.storm.analytics.handlers.OpenEventHandler;
import org.shirdrn.storm.analytics.handlers.PlayEndEventHandler;
import org.shirdrn.storm.analytics.handlers.PlayStartEventHandler;
import org.shirdrn.storm.api.ConnectionManager;
import org.shirdrn.storm.api.EventHandlerManager;
import org.shirdrn.storm.api.Result;
import org.shirdrn.storm.api.utils.IndicatorCalculatorFactory;
import org.shirdrn.storm.commons.constants.Keys;
import org.shirdrn.storm.commons.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.base.BaseRichSpout;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

public class RealtimeUtils {

	private static final Log LOG = LogFactory.getLog(RealtimeUtils.class);
	
	public static Configuration getDefaultConfiguration() {
		try {
			return new PropertiesConfiguration("config.properties");
		} catch (ConfigurationException e) {
			throw Throwables.propagate(e);
		}
	}
	
	public static Configuration getConfiguration(String name) {
		try {
			return new PropertiesConfiguration(name);
		} catch (ConfigurationException e) {
			throw Throwables.propagate(e);
		}
	}
	
	public static synchronized Jedis newAvailableConnection(final ConnectionManager<Jedis> connectionManager) {
		return connectionManager.getConnection();
	}
	
	public static EventHandlerManager<Collection<Result>, Jedis, JSONObject> getEventHandlerManager() {
		// register calculators
		LOG.info("Registering indicator calculators:");
		registerCalculators();
		LOG.info("Registered.");
		
		EventHandlerManager<Collection<Result>, Jedis, JSONObject> eventHandlerManager = new JedisEventHandlerManager();
		registerInterestedEvents(eventHandlerManager);
		mappingEventHandlers(eventHandlerManager);
		eventHandlerManager.start();
		return eventHandlerManager;
	}
	
	/**
	 * Register interested events
	 * @param interested
	 */
	private static void registerInterestedEvents(EventHandlerManager<Collection<Result>, Jedis, JSONObject> eventHandlerManager) {
		eventHandlerManager.interestEvent(EventCode.INSTALL);
		eventHandlerManager.interestEvent(EventCode.OPEN);
		eventHandlerManager.interestEvent(EventCode.PLAY_START);
		eventHandlerManager.interestEvent(EventCode.PLAY_END);
	}
	
	private static void mappingEventHandlers(final EventHandlerManager<Collection<Result>, Jedis, JSONObject> eventHandlerManager) {
		// register mappings: event-->EventHandler
		eventHandlerManager.mapping(EventCode.PLAY_START, new PlayStartEventHandler(EventCode.PLAY_START));
		eventHandlerManager.mapping(EventCode.PLAY_END, new PlayEndEventHandler(EventCode.PLAY_END));
		eventHandlerManager.mapping(EventCode.OPEN, new OpenEventHandler(EventCode.OPEN));
		eventHandlerManager.mapping(EventCode.INSTALL, new InstallEventHandler(EventCode.INSTALL));
	}
	
	/**
	 * Register calculators
	 */
	public static void registerCalculators() {
		// register calculators
		// register basic calculators
		registerCalculator(UserDeviceInfoCalculator.class);
		registerCalculator(UserDynamicInfoCalculator.class);
		
		// register statistical calculators
		registerCalculator(OpenAUCalculator.class);
		registerCalculator(OpenNUCalculator.class);
		registerCalculator(OpenTimesCalculator.class);
		registerCalculator(PlayAUCalculator.class);
		registerCalculator(PlayAUDurationCalculator.class);
		registerCalculator(PlayNUCalculator.class);
		registerCalculator(PlayNUDurationCalculator.class);
		registerCalculator(PlayTimesCalculator.class);
	}
	
	private static void registerCalculator(Class<?> calculatorClazz) {
		IndicatorCalculatorFactory.registerCalculator(calculatorClazz);
	}
	
	public static JSONObject getUserInfo(final Jedis connection, String udid) {
		String userKey = Constants.USER_INFO_KEY_PREFIX + udid;
		String appId = connection.hget(userKey, UserInfoKeys.APP_ID);
		String channel = connection.hget(userKey, UserInfoKeys.CHANNEL);
		String version = connection.hget(userKey, UserInfoKeys.VERSION);
		String osType = connection.hget(userKey, UserInfoKeys.OS_TYPE);
		String fod = connection.hget(userKey, UserInfoKeys.FIRST_OPEN_DATE);
		String fpd = connection.hget(userKey, UserInfoKeys.FIRST_PLAY_DATE);
		String lod = connection.hget(userKey, UserInfoKeys.LATEST_OPEN_DATE);
		String lpd = connection.hget(userKey, UserInfoKeys.LATEST_PLAY_DATE);
		
		JSONObject user = new JSONObject();
		putKV(user, UserInfoKeys.APP_ID, appId);
		putKV(user, UserInfoKeys.CHANNEL, channel);
		putKV(user, UserInfoKeys.VERSION, version);
		putKV(user, UserInfoKeys.OS_TYPE, osType);
		putKV(user, UserInfoKeys.FIRST_OPEN_DATE, fod);
		putKV(user, UserInfoKeys.FIRST_PLAY_DATE, fpd);
		putKV(user, UserInfoKeys.LATEST_OPEN_DATE, lod);
		putKV(user, UserInfoKeys.LATEST_PLAY_DATE, lpd);
		return user;
	}
	
	private static void putKV(JSONObject user, String key, String value) {
		if(value != null) {
			user.put(key, value);
		}
	}
	
	public static boolean isInvalidUser(JSONObject user) {
		boolean isInvalid = false;
		if(user != null && !user.isEmpty()) {
			if(user.containsKey(UserInfoKeys.CHANNEL) 
					&& user.containsKey(UserInfoKeys.VERSION) 
					&& user.containsKey(UserInfoKeys.OS_TYPE)) {
				isInvalid = true;
			}
		}
		return isInvalid;
	}
	
	public static boolean isNewUserOpen(final Jedis connection, String udid, final JSONObject user, String eventDatetime) {
		String key = Constants.USER_DYNAMIC_INFO_KEY_PREFIX + udid;
		String latestOpenDate = connection.hget(key, UserInfoKeys.LATEST_OPEN_DATE);
		boolean isNewUserOpen = true;
		if(latestOpenDate != null) {
			String eventDate = DateTimeUtils.format(eventDatetime, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
			long days = DateTimeUtils.getDaysBetween(latestOpenDate, eventDate, Constants.DT_DATE_PATTERN);
			if(days > 180) {
				isNewUserOpen = false;
			}
		}
		return isNewUserOpen;
	}
	
	public static boolean isNewUserPlay(final Jedis connection, String udid, final JSONObject user, String eventDatetime) {
		String key = Constants.USER_DYNAMIC_INFO_KEY_PREFIX + udid;
		String latestPlayDate = connection.hget(key, UserInfoKeys.LATEST_PLAY_DATE);
		boolean isNewUserPlay = true;
		if(latestPlayDate != null) {
			String eventDate = DateTimeUtils.format(eventDatetime, Constants.DT_EVENT_PATTERN, Constants.DT_DATE_PATTERN);
			long days = DateTimeUtils.getDaysBetween(latestPlayDate, eventDate, Constants.DT_DATE_PATTERN);
			if(days > 180) {
				isNewUserPlay = false;
			}
		}
		return isNewUserPlay;
	}
	
	
	public static BaseRichSpout newKafkaSpout(String topic, Configuration config) {
		String[] zks = config.getStringArray(Keys.KAFKA_ZK_SERVERS);
		String zkServers = Strings.join(zks, ",");
		String zkRoot = config.getString(Keys.STORM_ZK_ROOT); 
		boolean forceFromStart = config.getBoolean(Keys.STORM_KAFKA_FORCE_FROM_START, false);
		String clientId = config.getString(Keys.KAFKA_CLIENT_ID);
		Preconditions.checkArgument(clientId != null, "Kafka client ID MUST NOT be null!");
		
		// Configure Kafka
		BrokerHosts brokerHosts = new ZkHosts(zkServers);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, clientId);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = forceFromStart;
		
		// Configure Storm
		String[] zkHosts = config.getStringArray(Keys.STORM_ZK_HOSTS);
		int zkPort = config.getInt(Keys.STORM_ZK_PORT, 2181);
		spoutConf.zkServers = Arrays.asList(zkHosts);
		spoutConf.zkPort = zkPort;
		return new KafkaSpout(spoutConf);
	}
	
	
	private static final String CMD_PREFIX = "COMMAMD => ";
	private static final Map<Level, Integer> LOG_LEVEL_MAP = Maps.newHashMap();
	
	static {
		LOG_LEVEL_MAP.put(Level.DEBUG, Level.DEBUG_INT);
		LOG_LEVEL_MAP.put(Level.INFO, Level.INFO_INT);
		LOG_LEVEL_MAP.put(Level.WARN, Level.WARN_INT);
		LOG_LEVEL_MAP.put(Level.ERROR, Level.ERROR_INT);
		LOG_LEVEL_MAP.put(Level.FATAL, Level.FATAL_INT);
	}
	
	public static Level parseLevel(String level) {
		if(!Strings.isNullOrEmpty(level)) {
			level.toUpperCase();
		}
		return Level.toLevel(level);
	}
	
	public static void printRedisCmd(Log log, String cmd) {
		printRedisCmd(log, Level.INFO, cmd);
	}
	
	public static void printRedisCmd(String cmd) {
		printRedisCmd(LOG, cmd);
	}
	
	public static void printRedisCmd(Log log, Level level, String cmd) {
		int levelCode = LOG_LEVEL_MAP.get(level);
		String message = CMD_PREFIX + cmd;
		
		switch(levelCode) {
			case Level.DEBUG_INT:
				log.debug(message);
				break;
				
			case Level.INFO_INT:
				log.info(message);
				break;
				
			case Level.WARN_INT:
				log.warn(message);
				break;
				
			case Level.ERROR_INT:
				log.error(message);
				break;
				
			case Level.FATAL_INT:
				log.fatal(message);
				break;
				
			default:
				log.debug(message);
		}
	}
	
	public static int getDefaultExpireTime() {
		return Constants.CACHE_ITEM_EXPIRE_TIME;
	}
	
	public static int getExpireTime() {
		return DateTimeUtils.getDiffToNearestHour();
	}
	
	public static int getExpireTime(long timestamp) {
		return DateTimeUtils.getDiffToNearestHour(timestamp);
	}
}
