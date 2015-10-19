package com.xiaomi.storm.kafka.common;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.tuple.Fields;

public interface IRawMultiScheme extends Serializable {
	public Iterable<List<Object>> deserialize(byte[] ser) throws UnsupportedEncodingException;
	public Fields getOutputFields();
}
