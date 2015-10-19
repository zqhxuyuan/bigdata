package com.xiaomi.storm.kafka.common;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class MongoDBHelper {
	
	public static DBCollection getDBCollection(
			String mongoHost, 
			int mongoPort, 
			String dbName,
			String collectionName
			) throws UnknownHostException {
		DBCollection collection;
		Mongo mongo = new Mongo(mongoHost, mongoPort);
		DB db = mongo.getDB(dbName);
		collection = db.getCollection(collectionName);
		return collection;
	}
	
	public static DBCollection createCappedDBCollection(
			String mongoHost,
			int mongoPort,
			String dbName,
			String collectionName
			) throws UnknownHostException {
		DBCollection collection;
		Mongo mongo = new Mongo(mongoHost, mongoPort);
		mongo.dropDatabase(dbName);
		DB db = mongo.getDB(dbName);
		collection = db.createCollection(
				collectionName, 
				new BasicDBObject("capped", true)
				.append("size", 10000000));
		return collection;
	}
	
}
