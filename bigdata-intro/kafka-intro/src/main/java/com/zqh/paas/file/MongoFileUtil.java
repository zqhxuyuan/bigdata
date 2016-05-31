package com.zqh.paas.file;

import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.file.IFileManager;

public class MongoFileUtil {
	private static IFileManager iFileManager = null;
	private MongoFileUtil(){
		
	}
	private static IFileManager getInstance(){
		if(iFileManager!=null) return iFileManager;
        return  (IFileManager)PaasContextHolder.getContext().getBean("fileManager");
	}
	/**
	 * 适用于本地文件保存到mongodb
	 * @param fileName 文件路径＋文件名
	 * @param fileType 文件类型
	 * @return 保存后，产生的唯一标识
	 */
	public static String saveFile(String fileName, String fileType){
		return getInstance().saveFile(fileName, fileType);
	}
	/**
	 * 可以适用于页面提交过来的文件、本地文件等等
	 * @param byteFile 文件字节流
	 * @param fileName 文件名
	 * @param fileType 文件类型
	 * @return 保存后，产生的唯一标识
	 */
	public static String saveFile(byte[] byteFile, String fileName, String fileType){
		return getInstance().saveFile(byteFile, fileName, fileType);
	}

	/**
	 * @param fileId  文件ID，唯一标识
	 * @return 文件字节流
	 */
	public static byte[] readFile(String fileId){
		return getInstance().readFile(fileId);
	}

	/**
	 * @param fileId 文件ID，唯一标识
	 * @param localFileName 保存在本地的路径＋文件名
	 */
	public static void readFile(String fileId, String localFileName){
		getInstance().readFile(fileId, localFileName);
	}

	/**
	 * @param fileId 文件ID，唯一标识
	 */
	public static void deleteFile(String fileId){
		getInstance().deleteFile(fileId);
	}

	/**
	 * @param fileName 文件名
	 * @return
	 */
	public static byte[] readFileByName(String fileName){
		return getInstance().readFileByName(fileName);
	}

	/**
	 * @param fileName 文件名
	 * @param localFileName 保存在本地的路径＋文件名
	 */
	public static void readFileByName(String fileName, String localFileName){
		getInstance().readFileByName(fileName, localFileName);
	}

	/**
	 * @param fileName 文件名
	 */
	public static void deleteFileByName(String fileName){
		getInstance().deleteFileByName(fileName);
	}
	
	/**
	 * @param fileId 文件
	 */
	public static String getFileName(String fileId){
		return getInstance().getFileName(fileId);
	}
}
