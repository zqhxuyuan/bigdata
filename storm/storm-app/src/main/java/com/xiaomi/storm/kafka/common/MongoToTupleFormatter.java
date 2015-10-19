package com.xiaomi.storm.kafka.common;

import backtype.storm.tuple.Tuple;

import com.mongodb.DBObject;

import java.io.Serializable;
import java.util.List;


public abstract class MongoToTupleFormatter implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public abstract DBObject mapTupleToDBObject(DBObject object, Tuple tuple);
	
}
