package com.github.shuliga.utils;

import java.io.File;

/**
 * User: yshuliga
 * Date: 14.01.14 14:55
 */
public class FileUtil {

	public static final String PERSISTENCE_ROOT = "/opt/storm/ss_out";

	public static final String JSON_EXT = ".json";

	public static String createJsonPath(String rootPath, String eventId, String sourceType) {
		return rootPath + File.separator + sourceType + "_" + eventId + JSON_EXT;
	}


}
