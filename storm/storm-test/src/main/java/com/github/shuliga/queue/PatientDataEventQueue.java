package com.github.shuliga.queue;

import com.github.shuliga.data.PharmacyDataSource;
import com.github.shuliga.data.PharmacyEventData;
import com.github.shuliga.event.patient.PharmacyDataEvent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: yshuliga
 * Date: 06.01.14 12:41
 */
public class PatientDataEventQueue extends LinkedBlockingQueue<PharmacyDataEvent> {

	public static final int CAPACITY = 100;
	public static final long TIMEOUT = 2L;

	private static volatile PatientDataEventQueue instance;

	public static PatientDataEventQueue getInstance(){
		if (instance == null) {
			synchronized (PatientDataEventQueue.class){
				if (instance == null) {
					instance = new PatientDataEventQueue();
				}
			}
		}
		return instance;
	}

	private PatientDataEventQueue(){
		super(CAPACITY);
		fillQueue();
	}

	private void fillQueue() {
		PharmacyDataSource dataSource = new PharmacyDataSource();
		for (int i = 1; i <= CAPACITY; i++){
			offerEvent(createEvent(dataSource.nextRandomData()));
		}

	}

	private PharmacyDataEvent createEvent(PharmacyEventData eventData) {
		PharmacyDataEvent event = new PharmacyDataEvent(eventData.patientData.insuranceId, eventData);
		return event;
	}

	public boolean offerEvent(PharmacyDataEvent event){
		try {
			offer(event, TIMEOUT, TimeUnit.SECONDS);
			return true;
		} catch (InterruptedException e) {
			System.out.println("Queue offering timeout (" + TIMEOUT + " sec.)");
		}
		return false;
	}

	public PharmacyDataEvent pollEvent(){
		try {
			return poll(TIMEOUT, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			System.out.println("Queue polling timeout (" + TIMEOUT + " sec.)");
		}
		return null;
	}

}
