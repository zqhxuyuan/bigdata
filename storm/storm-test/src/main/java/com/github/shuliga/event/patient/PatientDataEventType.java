package com.github.shuliga.event.patient;

import com.github.shuliga.event.common.EventType;

/**
 * User: yshuliga
 * Date: 06.01.14 11:53
 */
public class PatientDataEventType extends EventType {

	public final String typeName = "PATIENT_DATA_EVENT";

	@Override
	public String getTypeName() {
		return typeName;
	}
}
