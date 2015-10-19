package com.xiaomi.storm.kafka.common;

import backtype.storm.tuple.Tuple;

import com.mongodb.DBObject;

import java.io.Serializable;

public abstract class UpdateQueryCreator implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public abstract DBObject createQuery(Tuple tuple);
}
