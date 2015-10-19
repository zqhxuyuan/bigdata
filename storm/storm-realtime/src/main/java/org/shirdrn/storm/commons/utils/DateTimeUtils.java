package org.shirdrn.storm.commons.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;

import com.google.common.collect.Lists;

public class DateTimeUtils {
	
	private static final long MILLS_PER_DAY = 1 * 24 * 60 * 60 * 1000;
	
	public static String format(Date date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
	
    public static Date parse(String dateStr, String pattern) {
        DateFormat format = new SimpleDateFormat(pattern);
        Date date = null;
        try {
            date = format.parse(dateStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

	public static String format(long timestamp, String format) {
		Date date = new Date(timestamp);
		return format(date, format);
	}
	
	public static String format(Date date, int field, int amount, String format) {
		GregorianCalendar gc = new GregorianCalendar();
		gc.setTime(date);
		gc.add(field, amount);
		return format(gc.getTime(), format);
	}
	
	public static long getDaysBetween(String date1, String date2, String format) {
		Date d1 = parse(date1, format);
		Date d2 = parse(date2, format);
		return Math.abs((d2.getTime() - d1.getTime()) / MILLS_PER_DAY);
	}
	
	public static String format(String srcDatetime, String srcFormat, String dstFormat) {
		Date d = parse(srcDatetime, srcFormat);
		return format(d, dstFormat);
	}
	
	public static LinkedList<String> getLatestHours(int nHours, String format) {
		LinkedList<String> hours = Lists.newLinkedList();
		Calendar c = Calendar.getInstance();
		for (int i = 0; i < nHours; i++) {
			c.add(Calendar.HOUR_OF_DAY, -1);
			hours.add(format(c.getTime(), format));
		}
		return hours;
	}
	
	public static int getDiffToNearestHour() {
		return getDiffToNearestHour(System.currentTimeMillis());
	}
	
	public static int getDiffToNearestHour(long timestamp) {
		Date date = new Date(timestamp);
		return getDiffToNearestHour(date);
	}
	
	@SuppressWarnings("deprecation")
	public static int getDiffToNearestHour(Date date) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.HOUR_OF_DAY, 1);
		Date toDate = c.getTime();
		toDate.setMinutes(0);
		toDate.setSeconds(0);
		return (int) (toDate.getTime() - date.getTime());
	}

	public static void main(String[] args) {
		System.out.println(format(System.currentTimeMillis(), "yyyyMMddHHmmssSSS"));
		System.out.println(getDaysBetween("2015-01-09", "2015-01-10", "yyyy-MM-dd"));
		System.out.println(getLatestHours(3, "yyyyMMddHH"));
		
		Calendar c = Calendar.getInstance();
		c.add(Calendar.HOUR_OF_DAY, 0);
		System.out.println(format(c.getTime(), "yyyy-MM-dd HH:mm:ss"));
		System.out.println(getDiffToNearestHour());
	}
}
