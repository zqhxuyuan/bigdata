package com.xiaomi.storm.kafka.common;

import java.io.Serializable;

import com.xiaomi.storm.kafka.config.ConfigFile;

public class KafkaHostPort implements Serializable {

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return host.hashCode();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		KafkaHostPort other = (KafkaHostPort) obj;
		return host.equals(other.host) && port == other.port;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return host + ":" + port;
	}

	/**
	 * data structure to store kafka broker ip with port 
	 */
	public KafkaHostPort(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public KafkaHostPort(String host) {
		this.host = host;
		this.port = ConfigFile.KFK_SERV_PORT;
	}
	
	private static final long serialVersionUID = 1L;
	private String host;
	private int port;

}
