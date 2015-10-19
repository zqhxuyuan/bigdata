package com.xiaomi.storm.kafka.common;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;

public class DynamicBrokerFetcher {
	
	public static Logger LOG = LoggerFactory.getLogger(DynamicPartitionManagerKeeper.class);
	
	public DynamicBrokerFetcher(ZkState zkState, SpoutConfigParser config) {
		//get curator to control zookeeper
		this._curator = zkState.getCurator();
		//LOG.info("@@@@@@@@@@@@@@@@##################$$$$$"+ this._curator);
		this._config = config;
	}
	
	@SuppressWarnings("all")
	public Map<String, List> getBrokerInfo() {
		
		/*
		 * get brokers from zookeeper
		 * broker -> ports, partitions
		 * maybe multi-brokers deploy on single machine
		 * 
		 * NOTICE
		 * kafka0.7.x topicKafkaPath = "/brokers/topics" + topic
		 * kafka0.8.x topicKafkaPath = "/brokers/topics/partitions" + topic
		 */
		
		Map<String, List> ret = new HashMap<String, List>();
		String topicKafkaPath = "/brokers/topics" + "/" + this._config.kafkaTopic;
		String kafkaInfoPath = "/brokers/ids";
		
		//LOG.info("@@@@@@@@@@@@@@@@topic kafka path:"+ topicKafkaPath);
		//LOG.info("@@@@@@@@@@@@@@@@kafka info path:"+ kafkaInfoPath);
		
		try {
			List<String> childs = this._curator.getChildren().forPath(topicKafkaPath);
			//List<String> childs = this._curator.getChildren().forPath(kafkaInfoPath);
			//LOG.info("@@@@@@@@@@@@@@@@get childrens"+ childs.toString());
			
			for(String c : childs) {
				//LOG.info("@@@@@@@@#####$$$$$$$$ ids id: " + c);
				
				byte[] numPartitions = this._curator.getData().forPath(topicKafkaPath + "/" + c);
				
				//LOG.info("@@@@@@@@#####$$$$$$$$ number:"+ numPartitions.toString());
				
				byte[] brokers = this._curator.getData().forPath(kafkaInfoPath + "/" + c);
				
				//LOG.info("@@@@@@@@#####$$$$$$$$ brokers:"+ brokers.toString());
				
				KafkaHostPort kafkaBrokers = getBrokers(brokers);
				int partitions = getPartitionsNumber(numPartitions);
				
				//LOG.info("@@@@@@@@#####$$$$$$$$ partitions:"+ partitions);
				
				List info = new ArrayList();
				info.add((int)kafkaBrokers.getPort());
				info.add((int)partitions);
				ret.put(kafkaBrokers.getHost(), info);
			}
		} catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
			// TODO Auto-generated catch block
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
		return ret;
	}
	
	private KafkaHostPort getBrokers(byte[] data) {
		/*
		 * kafka broker ex at zookeeper 
		 * .98-1373253791315:10.65.45.98:2000
		 */
		try {
			String[] brokerString = new String(data, "UTF-8").split(":");
			String hostname = brokerString[brokerString.length - 2];
			int port = Integer.parseInt(brokerString[brokerString.length - 1]);
			return new KafkaHostPort(hostname, port);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	private int getPartitionsNumber(byte[] data) {
		try {
			return Integer.parseInt(new String(data, "UTF-8"));
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}
	
	private CuratorFramework _curator;
	private SpoutConfigParser _config;
}
