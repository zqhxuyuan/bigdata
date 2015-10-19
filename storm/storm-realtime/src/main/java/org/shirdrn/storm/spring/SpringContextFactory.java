package org.shirdrn.storm.spring;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Basic spring context factory implementation. We use {@link ClassPathXmlApplicationContext}
 * as the context object class produced by this context factory instance, and invoke
 * {{@link #getContext(String)} method to obtain a context object cached in the factory.
 * 
 * @author yanjun
 */
public class SpringContextFactory extends AbstractContextFactory<ApplicationContext, ClassPathXmlApplicationContext> {

	private static final Log LOG = LogFactory.getLog(SpringContextFactory.class);
	
	public SpringContextFactory() {
		super();
	}
	
	@Override
	public synchronized void register(String name, String... configs) {
		try {
			if(!super.isContextExists(name)) {
				LOG.info("Initializing context for: name=" + name + ", configs=" + Arrays.asList(configs));
				ApplicationContext context = new ClassPathXmlApplicationContext(configs);
				super.register(name, context);
				super.register(name, context, ClassPathXmlApplicationContext.class);
				LOG.info("Context initialized: name=" + name + ", context=" + context);
			} else {
				throw new RuntimeException("Context object for name \"" + name + "\" existed!");
			}
		} catch (Exception e) {
			throw new RuntimeException("Error to create spring context: ", e);
		}
		
	}

}
