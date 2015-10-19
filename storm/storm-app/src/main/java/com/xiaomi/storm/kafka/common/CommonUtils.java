package com.xiaomi.storm.kafka.common;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import kafka.message.Message;

public class CommonUtils {
	
	public static String kafkaMsgToString(Message msg) {
		ByteBuffer buffer = msg.payload();
		byte[] bytes = new byte[buffer.remaining()];
		try {
			/*
			 * This method transfers bytes 
			 * from this buffer into the bytes array
			 */
			buffer.get(bytes);
		}catch (BufferUnderflowException e) {
			e.printStackTrace();
		}
		return new String(bytes);
	}
	
	public static List<String> getStaticHosts(String list) {
		List<String> zkHosts = new ArrayList<String>();
		String[] hosts = list.split(",");
		for (String host : hosts) {
			zkHosts.add(host);
		}
		return zkHosts;
	}	
	
	//store kafkahostport data structure with arraylist
	public static List<KafkaHostPort> convertHosts(List<String> hosts) {
		List<KafkaHostPort> ret = new ArrayList<KafkaHostPort>();
		for(String s: hosts) {
			KafkaHostPort khp;
			String[] spec = s.split(":");
			if(spec.length == 1) {
				khp = new KafkaHostPort(spec[0]);
			} else if(spec.length == 2) {
				khp = new KafkaHostPort(spec[0], Integer.parseInt(spec[1]));
			} else {
				throw new IllegalArgumentException("Invalid host specification: " + s);
			}
			ret.add(khp);
		}
		return ret;
	}
	
	public static String getUUID() {
		return UUID.randomUUID().toString();
	}
	 
}
