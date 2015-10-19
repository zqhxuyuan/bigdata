package org.shirdrn.storm.api.common;

import org.shirdrn.storm.api.CallbackHandler;
import org.shirdrn.storm.api.Result;

public abstract class GenericResult implements Result {

	private static final long serialVersionUID = 1L;
	protected int indicator;
	protected CallbackHandler<?> callbackHandler;

	@Override
	public <CONNECTION> void setCallbackHandler(CallbackHandler<CONNECTION> callbackHandler) {
		this.callbackHandler = callbackHandler;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <CONNECTION> CallbackHandler<CONNECTION> getCallbackHandler() {
		return (CallbackHandler<CONNECTION>) callbackHandler;
	}
	
	@Override
	public int getIndicator() {
		return indicator;
	}
	
	@Override
	public void setIndicator(int indicator) {
		this.indicator = indicator;
	}
	
	@Override
	public int compareTo(Result o) {
		if(this.indicator < o.getIndicator()) {
			return -1;
		}
		if(this.indicator > o.getIndicator()) {
			return 1;
		}
		return 1;
	}
}
