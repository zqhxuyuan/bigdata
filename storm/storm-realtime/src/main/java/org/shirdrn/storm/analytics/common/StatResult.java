package org.shirdrn.storm.analytics.common;

import org.shirdrn.storm.api.KeyCreateable;
import org.shirdrn.storm.api.common.GenericResult;
import org.shirdrn.storm.commons.constants.CommonConstants;

public class StatResult extends GenericResult implements KeyCreateable {

	private static final long serialVersionUID = 1L;
	private static final String NS_SEPARATOR = CommonConstants.REDIS_KEY_NS_SEPARATOR;
	
	private String strHour;
	private String osType;
	private String channel;
	private String version;
	
	public String getStrHour() {
		return strHour;
	}
	public void setStrHour(String strHour) {
		this.strHour = strHour;
	}
	public String getOsType() {
		return osType;
	}
	public void setOsType(String osType) {
		this.osType = osType;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	
	@Override
	public String createKey(String type) {
		// hkeys 2015111122::31::S
		// field 0::A-360::3.1.2
		// value 103
		
		// hkeys 2015111122::42::AU
		// field 0::A-360::3.1.2::YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
		// value Y
		return new StringBuffer()
			.append(strHour).append(NS_SEPARATOR)
			.append(indicator).append(NS_SEPARATOR)
			.append(type).toString();
	}
	
	public String toField() {
		return new StringBuffer()
			.append(indicator).append(NS_SEPARATOR)
			.append(osType).append(NS_SEPARATOR)
			.append(channel).append(NS_SEPARATOR)
			.append(version).toString();
	}
	
	@Override
	public String toString() {
		return new StringBuffer()
			.append("[indicator=").append(indicator).append(",")
			.append("strHour=").append(strHour).append(",")
			.append("osType=").append(osType).append(",")
			.append("channel=").append(channel).append(",")
			.append("version=").append(version).append("]").toString();
	}
	
}
