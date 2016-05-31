package com.zqh.paas.log;

import java.util.Map;

import net.sf.json.JSONObject;



public interface ILogWriter {
	public void write(String log);

	public void write(JSONObject logJson);

	@SuppressWarnings("rawtypes")
	public void write(Map logMap);
}
