package com.github.shuliga.event.patient;

import com.github.shuliga.data.PatientData;
import com.github.shuliga.event.common.AbstractEvent;
import com.github.shuliga.event.common.EventType;

/**
 * User: yshuliga
 * Date: 06.01.14 11:58
 */
public abstract class PatientDataEvent extends AbstractEvent {

	public String insuranceId;

	public PatientDataEvent(String insuranceId, PatientData payload){
		super(payload, "PatientEventStream");
		this.insuranceId = insuranceId;
	}

	@Override
	protected EventType createEventType() {
		return new PatientDataEventType();
	}

	public abstract String getRule();

}
