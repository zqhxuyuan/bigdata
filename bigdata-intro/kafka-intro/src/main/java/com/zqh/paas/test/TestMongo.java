package com.zqh.paas.test;

import com.zqh.paas.PaasContextHolder;
import com.zqh.paas.file.IFileManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static com.zqh.paas.file.MongoFileUtil.*;

/**
 * Created by zqhxuyuan on 15-3-9.
 */
public class TestMongo {

    public static void main(String[] args) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "clientSenderContext.xml" });
        IFileManager manager = (IFileManager) PaasContextHolder.getContext().getBean("fileManager");
		System.out.println(manager.saveFile("d:/jarscan.jar", "jar"));
        manager.readFileByName("mongo-2.6.1.jar","/Users/liwenxian/Downloads/aaaa.jar");
		manager.deleteFileByName("mongo-2.6.1.jar");
        System.out.println(manager.readFile("53a13d2d7e3401fd7757df7b"));
        manager.readFile("53980fa930047481d096ef2c","/Users/liwenxian/Downloads/cccc.jar");
		manager.deleteFile("53980fa930047481d096ef2c");
    }

    public void testMongoUtil(){
        String fileId = saveFile("/home/hadoop/data/helloworld.txt","txt");
        readFileByName("helloworld.txt","/home/hadoop/data/helloworld_mongo.txt");
        deleteFileByName("helloworld.txt");

        String fileId2 = saveFile("/home/hadoop/data/helloworld2.txt","txt");
        readFile(fileId2, "/home/hadoop/data/helloworld_mongoid.txt");
        System.out.println(getFileName(fileId2));
        deleteFile(fileId2);
    }
}
