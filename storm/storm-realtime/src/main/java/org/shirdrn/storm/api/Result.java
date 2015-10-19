package org.shirdrn.storm.api;

import java.io.Serializable;

/**
 * Package useful event data and a {@link CallbackHandler} object
 * after computing for a specified indicator.
 * 
 * @author Yanjun
 */
public interface Result extends Comparable<Result>, Serializable {

	<CONNECTION> void setCallbackHandler(CallbackHandler<CONNECTION> callbackHandler);
	
	<CONNECTION> CallbackHandler<CONNECTION> getCallbackHandler();
	
	int getIndicator();
	
	void setIndicator(int indicator);

}
