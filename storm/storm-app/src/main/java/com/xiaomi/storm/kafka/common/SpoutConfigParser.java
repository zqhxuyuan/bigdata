package com.xiaomi.storm.kafka.common;

import java.io.Serializable;
import java.util.List;

import com.xiaomi.storm.kafka.common.KafkaHostPort;
import com.xiaomi.storm.kafka.config.ConfigFile;
import com.xiaomi.storm.kafka.common.CommonUtils;

public class SpoutConfigParser implements Serializable {

	/**
	 * config parser
	 */
	
	public SpoutConfigParser() {}
	
	public void forceStartOffset(long millis) {
		this.startOffset = millis;
		if(-1 == this.startOffset) {
			forceFromStart = true;
		}
	}
	
	private static final long serialVersionUID = 1L;
	
	//read zookeeper config parameters
	public List<String> zkServers = CommonUtils
			.getStaticHosts(ConfigFile.ZK_HOSTS);
	public Integer zkPort = ConfigFile.ZK_PORT;
	public String zkRoot = ConfigFile.ZK_ROOT;
	public long stateUpdateIntervalsMs = ConfigFile.ZK_STATE_UPDATE_INTERVAL_MS;
	
	//read kafka config parameters
	public List<String> kafkaHosts = CommonUtils
			.getStaticHosts(ConfigFile.KFK_SERV_HOSTS);
	public List<KafkaHostPort> kafkaBrokerPairs = CommonUtils
			.convertHosts(kafkaHosts);
	public int kafkaPort = ConfigFile.KFK_SERV_PORT;
	public int partitionsPerHost = ConfigFile.KFK_PART_NUMS;
	public int fetchSizeBytes = ConfigFile.KFK_BUFFER_SIZE;
	public int bufferSizeBytes = ConfigFile.KFK_BUFFER_SIZE;
	public int socketTimeoutMs = ConfigFile.KFK_CONN_TIMEOUT_MS;
	public String kafkaTopic = ConfigFile.KFK_TOPIC;
	public String kafkaGroupId = ConfigFile.KFK_GROUP_ID;	
	public int pm_refresh_secs = ConfigFile.PM_REFRESH_SECS;
	
	public long startOffset = ConfigFile.KFK_START_OFFSET;
	public boolean forceFromStart = ConfigFile.IS_KFK_FORCE_START;
	
	public IRawMultiScheme scheme = new StringScheme();
	
	//read mongo config parameters
	public String mongoHost = ConfigFile.MONGO_HOST;
	public int mongoPort = ConfigFile.MONGO_PORT;
	public String boltDBName = ConfigFile.MONGO_DB_NAME;
	public String boltCollectionName = ConfigFile.MONGO_COLL_NAME;
	public String lostYear = ConfigFile.LOG_LOST_YEAR;
	public String logAccountKey = ConfigFile.LOG_ACCOUNT_KEY;
	public String logAccountValue = ConfigFile.LOG_ACCOUNT_VALUE;
	public long intervalTime = ConfigFile.LOG_EMIT_INTERVAL_MS;
	
	//read storm config parameters
	public boolean debugMode = ConfigFile.STORM_DEBUG;
}
