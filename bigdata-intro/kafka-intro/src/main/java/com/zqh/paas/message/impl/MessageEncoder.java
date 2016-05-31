package com.zqh.paas.message.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class MessageEncoder implements Encoder<String> {
	
	public MessageEncoder(VerifiableProperties props) {
		
	}

	public byte[] toBytes(String msg) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(msg);
			return bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

}
