package org.shirdrn.storm.commons.utils;

import java.util.List;

public class TopologyUtils {

	public static String toString(String topologyName, List<String> componentNames) {
		StringBuffer sb = new StringBuffer();
		sb.append(topologyName + "[");
		for(int i=0; i<componentNames.size()-1; i++) {
			sb.append("(" + componentNames.get(i) + ") -> ");
		}
		sb.append("(" + componentNames.get(componentNames.size()-1) + ")]");
		return sb.toString();
	}
	
	public static String toString(String topologyName, String... componentNames) {
		StringBuffer sb = new StringBuffer();
		sb.append(topologyName + "[");
		for(int i=0; i<componentNames.length-1; i++) {
			sb.append("(" + componentNames[i] + ") -> ");
		}
		sb.append("(" + componentNames[componentNames.length-1] + ")]");
		return sb.toString();
	}
}
