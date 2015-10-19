package org.shirdrn.storm.analytics.mydis.workers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncServer;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncWorker;
import org.shirdrn.storm.analytics.mydis.common.UpdatePolicy;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Maps;

public class UserSyncWorker extends RedisSyncWorker {

	private static final Log LOG = LogFactory.getLog(UserSyncWorker.class);
	protected int indicator;
	protected String userType;
	
	// PRIMARY KEY(indicator, hour)
	private static final String SQL = 
			"INSERT INTO realtime_db.realtime_user_result(indicator, hour, os_type, channel, version, count) " + 
			"VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE count=?" ;
	
	private final UpdatePolicy<Entry<StatObj, AtomicLong>> updatePolicy = new UserUpdatePolicy();
	
	public UserSyncWorker(RedisSyncServer syncServer, int indicator, String userType) {
		super(syncServer);
		this.indicator = indicator;
		this.userType = userType;
	}

	@Override
	public void process(Jedis connection) throws Exception {
		LinkedList<String> hours = DateTimeUtils.getLatestHours(latestHours, Constants.DT_HOUR_FORMAT);
		super.addCurrentHour(hours);
		LOG.info("Sync for hours: " + hours);
		
		for(final String hour : hours) {
			Map<StatObj, AtomicLong> statMap = Maps.newHashMap();
			LOG.info("Process: indicator=" + indicator + ", hour=" + hour);
			// like: 
			// key   -> 2311010202::32::AU
			// field -> 0::A-360::3.1.2::AAAAAAAAAADDDDDDDDD
			// value -> Y
			try {
				compute(connection, statMap, hour);
			} catch (Exception e) {
				LOG.error("", e);
			}
			if(!statMap.isEmpty()) {
				updatePolicy.computeBatch(SQL, statMap.entrySet());
			}
		}
	}

	private void compute(Jedis connection, Map<StatObj, AtomicLong> statMap,
			final String hour) throws Exception {
		String key = hour + CommonConstants.REDIS_KEY_NS_SEPARATOR + 
				indicator + CommonConstants.REDIS_KEY_NS_SEPARATOR + userType; 
		// TODO
		// performance consideration:
		// fetch results in batch, rather than fetch all once
		Set<String> fields = connection.hkeys(key);
		if(fields != null) {
			for(String field : fields) {
				LOG.info("key=" + key + ", field=" + field);
				String[] fieldValues = field.split(CommonConstants.REDIS_KEY_NS_SEPARATOR);
				if(fieldValues.length == 4) {
					final int osType = Integer.parseInt(fieldValues[0]); 
					final String channel = fieldValues[1];
					final String version = fieldValues[2];
					StatObj so = new StatObj(hour, osType, channel, version);
					AtomicLong c = statMap.get(so);
					if(c == null) {
						c = new AtomicLong(0);
						statMap.put(so, c);
					}
					statMap.get(so).incrementAndGet();
				}
			}
		}
	}
	
	class UserUpdatePolicy implements UpdatePolicy<Entry<StatObj, AtomicLong>> {

		@Override
		public void computeBatch(String sql, final Collection<Entry<StatObj, AtomicLong>> values) throws Exception {
			jdbcTemplate.execute(sql, new PreparedStatementCallback<int[]>() {

				@Override
				public int[] doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
					for(Entry<StatObj, AtomicLong> entry :values) {
						StatObj statObj = entry.getKey();
						long count = entry.getValue().get();
						ps.setInt(1, indicator);
						ps.setString(2, DateTimeUtils.format(statObj.hour, Constants.DT_HOUR_FORMAT, Constants.DT_MINUTE_FORMAT));
						ps.setInt(3, statObj.osType);
						ps.setString(4, statObj.channel);
						ps.setString(5, statObj.version);
						ps.setLong(6, count);
						ps.setLong(7, count);
						ps.addBatch();
					}
					return ps.executeBatch();
				}
				
			});			
		}
		
	}
	
	class StatObj {
		final String hour;
		final int osType;
		final String channel;
		final String version;
		
		public StatObj(String hour, int osType, String channel, String version) {
			super();
			this.hour = hour;
			this.osType = osType;
			this.channel = channel;
			this.version = version;
		}

		@Override
		public int hashCode() {
			return 31 * osType + 31 * channel.hashCode() + 31 * version.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			StatObj o = (StatObj) obj;
			return this.hashCode() == o.hashCode();
		}
		
		@Override
		public String toString() {
			return new StringBuffer()
				.append("hour=").append(hour).append(",")
				.append("osType=").append(osType).append(",")
				.append("channel=").append(channel).append(",")
				.append("version=").append(version).toString();
		}
	}

}
