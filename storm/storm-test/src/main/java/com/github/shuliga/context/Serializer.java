package com.github.shuliga.context;

import org.apache.commons.io.IOUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * User: yshuliga
 * Date: 11.03.14 12:51
 */
public class Serializer {

	public Serializer() {
	}

	public byte[] toBytes(Serializable obj){
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			return bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(oos);
		}
		return new byte[0];
	}

	public <O> O fromBytes(byte[] obj){
		O result = null;
		ByteArrayInputStream bis = new ByteArrayInputStream(obj);
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(bis);
			result = (O) ois.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(ois);
		}
		return result;
	}
}
