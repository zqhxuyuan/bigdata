package com.github.shuliga.context;

import javax.jms.Connection;
import javax.jms.Queue;

/**
 * User: yshuliga
 * Date: 09.01.14 14:47
 */
public enum JMSContext {
	INSTANCE;

	public Connection connection;
	public Queue dataQueue;
	public Queue notificationQueue;
}
