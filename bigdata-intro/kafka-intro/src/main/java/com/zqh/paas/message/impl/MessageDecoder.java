package com.zqh.paas.message.impl;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import net.sf.json.JSONObject;

import com.zqh.paas.message.Message;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class MessageDecoder implements Decoder<Message> {

	public MessageDecoder(VerifiableProperties props) {

	}

	public Message fromBytes(byte[] paramArrayOfByte) {
		try {
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(paramArrayOfByte));
			return (Message) JSONObject.toBean(
					JSONObject.fromObject(ois.readObject()), Message.class);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public static void main(String[] args) {
	}
}
