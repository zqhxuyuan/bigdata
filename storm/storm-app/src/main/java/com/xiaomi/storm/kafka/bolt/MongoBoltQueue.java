package com.xiaomi.storm.kafka.bolt;

import backtype.storm.tuple.Tuple;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.LinkedBlockingQueue;

import com.xiaomi.storm.kafka.common.MongoToTupleFormatter;
import com.xiaomi.storm.kafka.common.UpdateQueryCreator;


public abstract class MongoBoltQueue implements Runnable, Serializable{
	
	private static final long serialVersionUID = 1L;

	public MongoBoltQueue(MongoToTupleFormatter formatter, DB db, Mongo mongo,
			WriteConcern writeConcern, DBCollection collection,
			LinkedBlockingQueue<Tuple> queue) {
		this.formatter = formatter;
		this.db = db;
		this.mongo = mongo;
		this.writeConcern = writeConcern;
		this.collection = collection;
		this.queue = queue;
	}
	
	public MongoBoltQueue(MongoToTupleFormatter formatter, DB db, Mongo mongo,
			WriteConcern writeConcern, DBCollection collection, UpdateQueryCreator updateQuery,
			LinkedBlockingQueue<Tuple> queue) {
		this.formatter = formatter;
		this.updateQuery = updateQuery;
		this.db = db;
		this.mongo = mongo;
		this.writeConcern = writeConcern;
		this.collection = collection;
		this.queue = queue;
	}

	@Override
	public void run() {
		//do get tuples from queue
		while(running.get()) {
			try {
				//get tuples from queue
				Tuple tuple = queue.poll();
				if(tuple != null) {
					execute(tuple);
				} else {
					Thread.sleep(10);
				}
			} catch (Exception e) {
				if(running.get()) throw new RuntimeException(e);
			}
		}
	}
	
	public void stopTask() {
		running.set(false);
	}
	
	public abstract void execute(Tuple tuple);
	
	
	private AtomicBoolean running = new AtomicBoolean(true);
	
	protected MongoToTupleFormatter formatter;
	protected UpdateQueryCreator updateQuery;
	protected DB db;
	protected Mongo mongo;
	protected WriteConcern writeConcern;
	protected DBCollection collection;
	
	protected LinkedBlockingQueue<Tuple> queue;

}
