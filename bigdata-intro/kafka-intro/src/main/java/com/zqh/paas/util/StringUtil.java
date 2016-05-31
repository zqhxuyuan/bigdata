package com.zqh.paas.util;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    public static boolean isBlank(String str) {
        if (null == str) {
            return true;
        }
        if ("".equals(str.trim())) {
            return true;
        }
        return false;
    }

    public static String toString(Object obj) {
        if (obj == null) {
            return "";
        }
        return obj.toString();
    }

    public static String restrictLength(String strSrc, int iMaxLength) {
        if (strSrc == null) {
            return null;
        }
        if (iMaxLength <= 0) {
            return strSrc;
        }
        String strResult = strSrc;
        byte[] b = null;
        int iLength = strSrc.length();
        if (iLength > iMaxLength) {
            strResult = strResult.substring(0, iMaxLength);
            iLength = iMaxLength;
        }
        while (true) {
            b = strResult.getBytes();
            if (b.length <= iMaxLength) {
                break;
            }
            iLength--;
            strResult = strResult.substring(0, iLength);
        }
        return strResult;
    }
    
    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random  random = new Random();
        StringBuffer buf = new StringBuffer();
        
        for(int i = 0 ;i < length ; i ++)
        {
         int num = random.nextInt(str.length());
         buf.append(str.charAt(num));
        }
        
        return buf.toString();
    }
	
	/**
	 * 左补齐
	 * @param target 目标字符串
	 * @param fix 补齐字符
	 * @param length 目标长度
	 * @return
	 */
	public static String lPad(String target, String fix, int length){
		if(target==null || fix ==null || !(target.length()<length))
			return target;
		StringBuffer newStr = new StringBuffer();
		for(int i=0; i<length-target.length(); i++){
			newStr.append(fix);
		}
		return newStr.append(target).toString();
	}
	
	/**
	 * 右补齐
	 * @param target 目标字符串
	 * @param fix 补齐字符
	 * @param length 目标长度
	 * @return
	 */
	public static String rPad(String target, String fix, int length){
		if(target==null || fix ==null || !(target.length()<length))
			return target;
		StringBuffer newStr = new StringBuffer();
		newStr.append(target);
		for(int i=0; i<length-target.length(); i++){
			newStr.append(fix);
		}
		return newStr.toString();
	}
	
	/**
	 * 字符串数据join操作
	 * @param strs
	 * @param spi
	 * @return
	 * @author zhoubo
	 */
	public static String join(String[] strs, String spi){
		StringBuffer buf = new StringBuffer();
		int step = 0;
		for(String str : strs){
			buf.append(str);
			if (step ++ < strs.length - 1)
				buf.append(spi);
		}
		return buf.toString();
	}
    
    //默认值为无
    public static String toString2(Object obj) {
        if (obj == null) {
            return "无";
        }
        return obj.toString();
    }
    /*public static void main(String[] args){
        System.out.println(StringUtil.getRandomString(10));
    }*/

    /**
     * 固网号码去除 区号-号码 中间的横杠
     * 010-88018802
     * @param str
     * @return
     * @author mayt
     */
    public static String replaceServiceNumBar(String str) {
        String dest = "";
        if (str!=null) {
            Pattern p = Pattern.compile("-");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }
    public static void main(String[] args) {
        System.out.println(replaceServiceNumBar("010-33883833"));
    }
}
