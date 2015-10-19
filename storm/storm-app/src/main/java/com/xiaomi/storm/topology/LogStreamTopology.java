package com.xiaomi.storm.topology;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import com.xiaomi.storm.kafka.bolt.MongoAccoutBolt;
import com.xiaomi.storm.kafka.bolt.MongoInsertBolt;
import com.xiaomi.storm.kafka.common.InsertHelper;
import com.xiaomi.storm.kafka.common.MongoDBHelper;
import com.xiaomi.storm.kafka.common.MongoToTupleFormatter;
import com.xiaomi.storm.kafka.common.SpoutConfigParser;
import com.xiaomi.storm.kafka.spout.KafkaSpout;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class LogStreamTopology {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws UnknownHostException, AlreadyAliveException, InvalidTopologyException, InterruptedException {
		
		SpoutConfigParser config = new SpoutConfigParser(); 
				
		CountDownLatch latch = new CountDownLatch(1);
		
		DBCollection coll = MongoDBHelper.createCappedDBCollection(
				config.mongoHost, 
				config.mongoPort,
				config.boltDBName,
				config.boltCollectionName
				);
		if(null != coll) {
			InsertHelper inserter = new InsertHelper(
					config.mongoHost,
					config.mongoPort,
					config.boltDBName,
					config.boltCollectionName,
					latch);
			new Thread(inserter).start();
			
			TopologyBuilder builder = new TopologyBuilder();
			
			KafkaSpout kafkaSpout = new KafkaSpout(config);
			
			MongoAccoutBolt<Object> accoutBolt = new MongoAccoutBolt<Object>(
					config.lostYear,
					config.intervalTime,
					config.logAccountKey,
					config.logAccountValue
					); 
			
			MongoToTupleFormatter formatter = new MongoToTupleFormatter() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public DBObject mapTupleToDBObject(DBObject object, Tuple tuple) {
					
					return BasicDBObjectBuilder.start()
							.add("record", tuple.getStringByField("records"))
							.add("timestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new Date()))
							.get();
				}
				
			};
			
			MongoInsertBolt mongoInserter = new MongoInsertBolt("mongodb://" 
					+ config.mongoHost
					+ ":"
					+ config.mongoPort
					+ "/"
					+ config.boltDBName,
					config.boltCollectionName,
					formatter,
					WriteConcern.NONE,
					true
					);
					
			//config storm topology
			builder.setSpout(
					"xiaomi-log",
					kafkaSpout, 
					1);
			builder.setBolt(
					"account", 
					accoutBolt,
					1).allGrouping("xiaomi-log");
			builder.setBolt(
					"mongoInserter", 
					mongoInserter,
					1).fieldsGrouping("account", new Fields("records"));
			
			Config conf = new Config();
			conf.setDebug(config.debugMode);
			//System.out.println("########################## ok");			
			if(args != null && args.length > 0) {
				conf.setNumAckers(1);
				conf.setNumWorkers(1);
				StormSubmitter.submitTopology(
						args[0], 
						conf,
						builder.createTopology());
				latch.countDown();
			} else {
				LocalCluster cluster = new LocalCluster();
				
				//System.out.println("########################## ok");
				
				cluster.submitTopology(
						"mongoStorm", 
						conf, 
						builder.createTopology());
				
				latch.countDown();
				
				//Thread.sleep(10);
				
			}
			
		} else {
			System.exit(-1);
		}
		
	}

}
