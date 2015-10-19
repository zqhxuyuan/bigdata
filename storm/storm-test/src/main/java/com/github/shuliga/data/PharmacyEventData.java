package com.github.shuliga.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * User: yshuliga
 * Date: 06.01.14 11:21
 */
public class PharmacyEventData  implements Serializable {
	public PatientData patientData = new PatientData();
	public PharmacyData pharmacyData = new PharmacyData();
	public PrescriberData prescriberData = new PrescriberData();
	public List<MedicineData> medicineDataList = new ArrayList<MedicineData>();

}
