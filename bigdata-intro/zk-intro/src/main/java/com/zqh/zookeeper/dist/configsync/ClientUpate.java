package com.zqh.zookeeper.dist.configsync;

public class ClientUpate {
	
	//private static final String PATH = "/sanxian";

	public static void main(String[] args) throws Exception {
		ZKConfig conf = new ZKConfig();
		
		conf.addOrUpdateData("修真天劫，九死一生。");
		conf.addOrUpdateData("圣人之下，皆为蝼蚁，就算再大的蝼蚁，还是蝼蚁.");
		conf.addOrUpdateData("努力奋斗，实力才是王道！ ");

		conf.close();
	}
	
}
