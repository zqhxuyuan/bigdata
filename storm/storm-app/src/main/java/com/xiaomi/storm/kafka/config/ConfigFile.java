package com.xiaomi.storm.kafka.config;

public class ConfigFile {
	
	//Storm
	public static Integer STORM_SPOUT_PARALLELISM = 1;
	public static Integer STORM_BOLT_PARALLELISM = 1;
	public static boolean STORM_DEBUG = true;
	
	
	//Kafka
	public static String KFK_SERV_HOSTS = "10.201.19.6";
	public static int KFK_SERV_PORT = 9092;
	public static int KFK_BUFFER_SIZE = 5*1024*1024;
	public static int KFK_PART_NUMS = 1;
	public static String KFK_GROUP_ID = "gallery_log_1";
	public static String KFK_TOPIC = "gallery";
	public static int KFK_CONN_TIMEOUT_MS = 1000;
	
	/*
	 * Kakfa::SimpleConsumer::API
	 * getFirstOffset(topic,partition) send -2
	 * getLastOffset(topic,partition) send -1
	 */
	public static long KFK_START_OFFSET =  -2;
	public static boolean IS_KFK_FORCE_START = false;
	
	//Zookeeper
	public static String ZK_ROOT = "/xiaomi_micloud_gallery";
	public static String ZK_HOSTS = "10.201.19.1,10.201.19.2,10.201.19.3";
	public static int ZK_PORT = 2181;
	public static long ZK_STATE_UPDATE_INTERVAL_MS = 2000;
	
	//PartitionManager
	public static int PM_REFRESH_SECS = 60;
	
	//Mongodb
	public static String MONGO_HOST = "10.201.19.11";
	public static int MONGO_PORT = 8777;
	public static String MONGO_DB_NAME = "micloud";
	public static String MONGO_COLL_NAME = "gallery";
	
	//LOG ACCOUNT
	public static String LOG_LOST_YEAR = "2014";
	public static String LOG_ACCOUNT_KEY = "status_code";
	public static String LOG_ACCOUNT_VALUE = "request_time";
	public static long LOG_EMIT_INTERVAL_MS = 30;
	
}
