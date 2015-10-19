package com.github.shuliga.event.patient;

/**
 * User: yshuliga
 * Date: 06.01.14 11:53
 */
public class PharmacyDataEventType extends PatientDataEventType {

	public final String typeName = "PHARMACY_DATA_EVENT";

	@Override
	public String getTypeName() {
		return typeName;
	}
}
