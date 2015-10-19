package com.github.shuliga.data;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * User: yshuliga
 * Date: 06.01.14 11:32
 */
public class PharmacyDataSource {

	private final static String[] insuranceIds = {"12345670", "12345671", "12345672", "12345673"};
	private final static Random rnd = new Random();
	private Calendar cal = Calendar.getInstance();

	private Gson gson;
	public PharmacyDataSource(){
		gson = new Gson();
	}

	public String getNextJson(){
		PharmacyEventData data = nextRandomData();
		return gson.toJson(data);
	}

	public PharmacyEventData nextRandomData() {
		PharmacyEventData data = new PharmacyEventData();
		data.patientData.insuranceId = insuranceIds[rnd.nextInt(insuranceIds.length - 1)];
		data.pharmacyData.pharmacyId = (long) rnd.nextInt(10);
		data.medicineDataList = createMedicineList();
		return data;
	}

	private List<MedicineData> createMedicineList() {
		final int cnt = rnd.nextInt(10);
		List<MedicineData> medicineList = new ArrayList<MedicineData>();
		for (int i = 0; i < cnt; i++){
			medicineList.add(createMedicine());
		}
		return medicineList;
	}

	private MedicineData createMedicine() {
		MedicineData medicine = new MedicineData();
		medicine.csaSchedule = rnd.nextInt(4) + 1;
		medicine.fillDate = getRandomDaysBeforeToday(10);
		medicine.filledRefills = rnd.nextInt(10) + 1;
		return medicine;
	}

	private Date getRandomDaysBeforeToday(int days) {
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -rnd.nextInt(days) - 1);
		return cal.getTime();
	}

}
