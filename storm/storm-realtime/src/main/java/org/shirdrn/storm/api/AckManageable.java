package org.shirdrn.storm.api;

public interface AckManageable {

	void setDoAckManaged(boolean doAckManaged);
	
	boolean isDoAckManaged();
	
	void setDoAckFailureManaged(boolean doAckFailureManaged);
	
	boolean isDoAckFailureManaged();
}
