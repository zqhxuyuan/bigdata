package com.xiaomi.storm.kafka.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampConverter {

	public static long stdToUnixTimeStamp(String standard) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = format.parse(standard);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date.getTime()/1000;
	}
	
	public static String unixToStdTimeStamp(long unix) {
		Long unixTime = new Long(unix);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String date = format.format(new Date(unixTime * 1000));
		return date;
	}
}
