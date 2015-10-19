package com.xiaomi.storm.kafka.bolt;

import java.net.UnknownHostException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.*;

import com.xiaomi.storm.kafka.common.MongoToTupleFormatter;

public abstract class MongoBoltBase extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public MongoBoltBase(String url, String collectionName,
			MongoToTupleFormatter formatter, WriteConcern writeConcern) {
		this.url = url;
		this.collectionName = collectionName;
		this.formatter = formatter;
		this.writeConcern = writeConcern == null ? WriteConcern.NONE : writeConcern;
	}

	//bolt runtime object
	protected Map map;
	protected TopologyContext topoloyContext;
	protected OutputCollector collector;
	
	//constructor args
	protected String url;
	protected String collectionName;
	protected MongoToTupleFormatter formatter;
	protected WriteConcern writeConcern;
	
	//Mongo objects
	protected Mongo mongo;
	protected DB db;
	protected DBCollection collection;
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
		this.map = stormConf;
		this.topoloyContext = context;
		this.collector = collector;
		
		//do mongoDB connection
		
		try {
			MongoURI uri = new MongoURI(this.url);
			//open the db
			this.mongo = new Mongo(uri);
			//grab the db
			this.db = this.mongo.getDB(uri.getDatabase());
			//do authenticate
			if(uri.getUsername() != null) {
				this.db.authenticate(uri.getUsername(), uri.getPassword());
			}
			//grb the collection name
			this.collection = this.db.getCollection(collectionName);
		}catch (UnknownHostException e){
			throw new RuntimeException(e);
		} 
	}

	@Override
	public abstract void execute(Tuple input);
	
	@Override
	public abstract void cleanup();
	
	
}
