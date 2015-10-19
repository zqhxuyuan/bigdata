package org.shirdrn.storm.analytics.mydis.workers;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncServer;
import org.shirdrn.storm.analytics.mydis.common.RedisSyncWorker;
import org.shirdrn.storm.analytics.mydis.common.UpdatePolicy;
import org.shirdrn.storm.analytics.mydis.constants.Constants;
import org.shirdrn.storm.commons.constants.CommonConstants;
import org.shirdrn.storm.commons.constants.StatIndicators;
import org.shirdrn.storm.commons.utils.DateTimeUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementCallback;

import redis.clients.jedis.Jedis;

import com.google.common.collect.Lists;

public class StatSyncWorker extends RedisSyncWorker {

	private static final Log LOG = LogFactory.getLog(StatSyncWorker.class);
	private final int[] indicators = new int[] {
			StatIndicators.OPEN_NU, 
			StatIndicators.OPEN_AU, 
			StatIndicators.OPEN_TIMES,
			StatIndicators.PLAY_NU, 
			StatIndicators.PLAY_AU, 
			StatIndicators.PLAY_TIMES,
			StatIndicators.PLAY_NU_DURATION, 
			StatIndicators.PLAY_AU_DURATION
	};
	// PRIMARY KEY(indicator, hour, os_type, channel, version)
	private static final String SQL = 
			"INSERT INTO realtime_db.realtime_stat_result(indicator, hour, os_type, channel, version, count) " + 
			"VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE count=?" ;
	
	private final UpdatePolicy<StatRecord> updatePolicy = new StatUpdatePolicy();
	
	public StatSyncWorker(RedisSyncServer syncServer) {
		super(syncServer);
	}
	
	@Override
	public void process(Jedis connection) throws Exception {
		LinkedList<String> hours = DateTimeUtils.getLatestHours(latestHours, Constants.DT_HOUR_FORMAT);
		super.addCurrentHour(hours);
		LOG.info("Sync for hours: " + hours);
		
		for(final int indicator : indicators) {
			for(final String hour : hours) {
				LOG.info("Process: indicator=" + indicator + ", hour=" + hour);
				try {
					compute(connection, indicator, hour);
				} catch (Exception e) {
					LOG.warn("", e);
				}
			}
		}
	}

	private void compute(Jedis connection, final int indicator, final String hour) throws Exception {
		// like: 
		// key   -> 2311010202::22::S
		// value -> 0::A-360::3.1.2
		String key = hour + CommonConstants.REDIS_KEY_NS_SEPARATOR + 
				indicator + CommonConstants.REDIS_KEY_NS_SEPARATOR +
				CommonConstants.NS_STAT_HKEY; 
		Set<String> fields = connection.hkeys(key);
		if(fields != null) {
			final List<StatRecord> records = Lists.newArrayList();
			for(String field : fields) {
				LOG.info("key=" + key + ", field=" + field);
				String[] fieldValues = field.split(CommonConstants.REDIS_KEY_NS_SEPARATOR);
				String strCount = connection.hget(key, field);
				if(fieldValues.length == 3) {
					StatRecord record = new StatRecord();
					record.indicator = indicator;
					record.hour = hour;
					record.osType = Integer.parseInt(fieldValues[0]); 
					record.channel = fieldValues[1];
					record.version = fieldValues[2];
					record.count = Long.parseLong(strCount);
					records.add(record);
				}
			}
			// update in batch
			updatePolicy.computeBatch(SQL, records);
		}
	}
	
	class StatUpdatePolicy implements UpdatePolicy<StatRecord> {

		@Override
		public void computeBatch(String sql, final Collection<StatRecord> values) throws Exception {
			jdbcTemplate.execute(sql, new PreparedStatementCallback<int[]>() {

				@Override
				public int[] doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
					for(StatRecord r : values) {
						ps.setInt(1, r.indicator);
						ps.setString(2, DateTimeUtils.format(r.hour, Constants.DT_HOUR_FORMAT, Constants.DT_MINUTE_FORMAT));
						ps.setInt(3, r.osType);
						ps.setString(4, r.channel);
						ps.setString(5, r.version);
						ps.setLong(6, r.count);
						ps.setLong(7, r.count);
						ps.addBatch();
					}
					return ps.executeBatch();
				}
				
			});		
		}

	}
	
	class StatRecord {
		int indicator;
		String hour;
		int osType;
		String channel;
		String version;
		long count;
	}

}
