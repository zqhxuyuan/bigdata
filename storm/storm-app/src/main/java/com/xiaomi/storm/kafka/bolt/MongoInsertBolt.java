package com.xiaomi.storm.kafka.bolt;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.xiaomi.storm.kafka.common.MongoToTupleFormatter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/*
 * add tuple to queue [optional]
 * transform tuples to DBObjects and inserting into MongoDB 
*/


public class MongoInsertBolt extends MongoBoltBase {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private LinkedBlockingQueue<Tuple> queue  = new LinkedBlockingQueue<Tuple>(10000);
	private MongoBoltQueue task;
	private Thread writeThread;
	//open switch off queue fuction
	private boolean inThread;
	
	public MongoInsertBolt(String url, String collectionName,
			MongoToTupleFormatter formatter, WriteConcern writeConcern) {
		super(url, collectionName, formatter, writeConcern);
		// TODO Auto-generated constructor stub
	}
	
	public MongoInsertBolt(String url, String collectionName,
			MongoToTupleFormatter formatter, WriteConcern writeConcern, boolean inThread) {
		super(url, collectionName, formatter, writeConcern);
		this.inThread = inThread;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		super.declareOutputFields(declarer);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context, collector);
		//insert in a separate thread, inserting into mongodb async non blocking
		if (this.inThread) {
			this.task = new MongoBoltQueue(this.formatter, this.db, this.mongo, 
					this.writeConcern, this.collection, this.queue) {

						private static final long serialVersionUID = 1L;

						@Override
						public void execute(Tuple tuple) {
							// TODO Auto-generated method stub
							DBObject object = new BasicDBObject();
							this.collection.insert(this.formatter.mapTupleToDBObject(object, tuple)
									, this.writeConcern);
						}
			};
			this.writeThread = new Thread(this.task);
			this.writeThread.start();
		}
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(this.inThread) {
			//add tuples into queue
			this.queue.add(input);
		} else {
			try {
				//add code to handle tuples or subclass override
				DBObject object = this.formatter.mapTupleToDBObject(new BasicDBObject(), input);
				this.collection.insert(object, this.writeConcern);	
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		if(this.inThread)
			this.task.stopTask();
		else
			this.mongo.close();
	}

}
