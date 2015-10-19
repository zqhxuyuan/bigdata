package com.github.shuliga.context;

import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

/**
 * User: yshuliga
 * Date: 09.01.14 14:49
 */
public class JMSContextProducer {


	public static final String JMS_CONNECTION_FACTORY_NAME = "BSCA_POC_QueueConnectionFactory"; //"BSCA_POC";
	public static final String JMS_QUEUE_NAME = "jms_bsca_poc_DataEventQueue"; //"jms/bsca_poc/DataEventQueue";
	public static final String JMS_NOTIFICATION_QUEUE_NAME = "jms_bsca_poc_NotificationEventQueue";
	public static final String INITIAL_CONTEXT_FACTORY = "com.sun.jndi.fscontext.RefFSContextFactory";
	public static final String PROVIDER_URL = "file:///opt/java/my_broker";

	public JMSContextProducer(){
	}

	public void fillContext(JMSContext jmsContext){
		InitialContext context = null;
		Properties env = new Properties();
		env.put(Context.INITIAL_CONTEXT_FACTORY,INITIAL_CONTEXT_FACTORY);
		env.put(Context.PROVIDER_URL, PROVIDER_URL);
		try{
			context = new InitialContext(env);
			QueueConnectionFactory connectionFactory = (QueueConnectionFactory)context.lookup(JMS_CONNECTION_FACTORY_NAME);
			jmsContext.dataQueue = (Queue) context.lookup(JMS_QUEUE_NAME);
			jmsContext.notificationQueue = (Queue) context.lookup(JMS_NOTIFICATION_QUEUE_NAME);
			jmsContext.connection = connectionFactory.createConnection();
			System.out.println("Context Created");
		} catch (Exception e){
			Logger.getLogger(getClass().getName(), e.getMessage());
			e.printStackTrace();
		}

	}

}
