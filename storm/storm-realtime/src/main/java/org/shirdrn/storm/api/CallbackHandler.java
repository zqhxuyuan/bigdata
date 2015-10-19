package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Callback handler invoked when persisting data to the specified storage system.
 * 
 * @author Yanjun
 *
 * @param <CONNECTION> Connection object.
 */
public interface CallbackHandler<CONNECTION> extends Serializable {

	/**
	 * Invoke the callback computation logic.
	 * @param connection
	 * @throws Exception
	 */
	void callback(CONNECTION connection) throws Exception;
	
}
